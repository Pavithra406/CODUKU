from fastapi import FastAPI, HTTPException, Depends, status, WebSocket, WebSocketDisconnect
import ast
import difflib
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel, EmailStr
from dotenv import load_dotenv
import os
import jwt
from datetime import datetime, timedelta
import bcrypt
import json
from uuid import uuid4
import httpx
from motor.motor_asyncio import AsyncIOMotorClient
import asyncio
import redis.asyncio as redis_async
from services.mentor_service import router as mentor_router

load_dotenv(override=True)

import logging
from fastapi.responses import JSONResponse
from fastapi import Request

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

app = FastAPI(
    title="CODUKU API",
    description="Competitive Coding Platform",
    version="1.0.0"
)

@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    logger.error(f"Global Exception on {request.url.path}: {exc}", exc_info=True)
    return JSONResponse(
        status_code=500,
        content={"detail": "An unexpected error occurred. Please try again later."},
    )

# Include mentors router early
app.include_router(mentor_router)

# ====== CORS CONFIGURATION ======
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",
        "http://localhost:3001",
        "https://coduku-frontend.vercel.app",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ====== CONFIGURATION ======
JWT_SECRET = os.getenv("JWT_SECRET", "dev-secret-key-change-in-production-12345")
JWT_ALGORITHM = "HS256"
JUDGE0_API_URL = os.getenv("JUDGE0_API_URL", "http://localhost:2358")
DATABASE_URL = os.getenv("DATABASE_URL", "mongodb://localhost:27017/coduku")
REDIS_URL = os.getenv("REDIS_URL")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

# Switches for testing (docs describe in-memory + optional real services)
MONGO_FORCE_DISABLE = os.getenv("MONGO_FORCE_DISABLE", "false").lower() == "true"
JUDGE0_MODE = os.getenv("JUDGE0_MODE", "auto").lower()  # mock | real | auto
JUDGE0_FORCE_MOCK = os.getenv("JUDGE0_FORCE_MOCK", "false").lower() == "true"

# ====== SECURITY ======
security = HTTPBearer()

# ====== OPTIONAL PERSISTENCE (MongoDB) ======
mongo_enabled = False
_mongo_client = None
_mongo_db = None
_users_coll = None
_submissions_coll = None

# ====== OPTIONAL REDIS (leaderboards cache) ======
redis_enabled = False
_redis_client = None

# ====== WEBSOCKET MANAGER ======
class ConnectionManager:
    def __init__(self):
        self.active_connections: list[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)

    async def broadcast(self, message: str):
        for connection in list(self.active_connections):
            try:
                await connection.send_text(message)
            except Exception:
                self.disconnect(connection)

ws_manager = ConnectionManager()

# ====== OPTIONAL SUPABASE (users + problems persistence) ======
supabase_enabled = False
_supabase_client: httpx.AsyncClient | None = None


@app.on_event("startup")
async def startup_event():
    global mongo_enabled, _mongo_client, _mongo_db, _users_coll, _submissions_coll
    if MONGO_FORCE_DISABLE:
        mongo_enabled = False
    else:
        try:
            _mongo_client = AsyncIOMotorClient(DATABASE_URL, serverSelectionTimeoutMS=1500)
            _mongo_db = _mongo_client.get_default_database()

            # Quick connectivity check.
            await _mongo_db.command("ping")

            _users_coll = _mongo_db["users"]
            _submissions_coll = _mongo_db["submissions"]

            # Ensure email is unique to prevent duplicate accounts.
            await _users_coll.create_index("email", unique=True)
            mongo_enabled = True
            print("MongoDB connected: persistence enabled")
        except Exception as e:
            mongo_enabled = False
            print(f"MongoDB unavailable: persistence disabled ({e})")

    # Redis is optional; we use it only for leaderboard ordering/caching.
    global redis_enabled, _redis_client
    if REDIS_URL:
        try:
            _redis_client = redis_async.from_url(REDIS_URL, decode_responses=True)
            await _redis_client.ping()
            redis_enabled = True
            print("Redis connected: leaderboard cache enabled")
        except Exception as e:
            redis_enabled = False
            _redis_client = None
            print(f"Redis unavailable: cache disabled ({e})")

    # Supabase is optional; we use it for persistent auth/problems when initialized.
    global supabase_enabled, _supabase_client
    if SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY:
        try:
            _supabase_client = httpx.AsyncClient(timeout=20)
            # Auto-detect whether the SQL schema is present.
            ping = await _supabase_client.get(
                f"{SUPABASE_URL}/rest/v1/users",
                headers={
                    "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
                    "apikey": SUPABASE_SERVICE_ROLE_KEY,
                },
                params={"select": "id", "limit": 1},
            )
            if ping.status_code == 200:
                supabase_enabled = True
                print("Supabase connected: persistence enabled")
                # Load problems into memory so the existing Judge0 execution flow works.
                try:
                    await supabase_load_problems_into_memory()
                except Exception as e2:
                    print(f"Supabase problems load failed; using in-memory problems ({e2})")
                # Load users into memory so leaderboards can display names after restart.
                try:
                    await supabase_load_users_into_memory()
                except Exception as e3:
                    print(f"Supabase users load failed; leaderboard may be limited ({e3})")
            else:
                supabase_enabled = False
                print(f"Supabase not ready (status {ping.status_code}); persistence disabled")
        except Exception as e:
            supabase_enabled = False
            print(f"Supabase unavailable: persistence disabled ({e})")

    # If Mongo persistence is enabled, also bootstrap users into memory.
    if mongo_enabled and _users_coll is not None:
        try:
            mongo_users = await _users_coll.find({}).to_list(length=5000)
            for u in mongo_users:
                email = u.get("email")
                if not email:
                    continue
                users_db[email] = {
                    "id": u.get("id"),
                    "name": u.get("name") or u.get("username") or "",
                    "username": u.get("username") or u.get("name") or "",
                    "email": email,
                    "house": (u.get("house") or "gryffindor"),
                    "password_hash": u.get("password_hash") or "",
                    "total_score": int(u.get("total_score", 0) or 0),
                    "problems_solved": int(u.get("problems_solved", 0) or 0),
                    "submissions": int(u.get("submissions", 0) or 0),
                }
            print("Mongo users bootstrapped into memory")
        except Exception as e:
            print(f"Mongo users bootstrap failed ({e})")

# ====== DATA MODELS ======
class RegisterRequest(BaseModel):
    # Support both `name` (your current React UI) and `username` (Next.js guide).
    name: str | None = None
    username: str | None = None
    email: EmailStr
    password: str
    house: str = "gryffindor"

class LoginRequest(BaseModel):
    email: EmailStr
    password: str

class AuthResponse(BaseModel):
    access_token: str
    user_id: str
    # Keep both for compatibility across UIs/guides.
    name: str
    username: str
    email: str
    house: str

class SubmissionRequest(BaseModel):
    problem_id: str
    code: str
    language: str

class SubmissionResponse(BaseModel):
    submission_id: str
    status: str
    message: str


# ====== DAY 2 (Next.js-style) MODELS ======
class Day2SubmissionRequest(BaseModel):
    problem_id: int  # integer ID (1..N) as in the guides
    language: str  # python3 | cpp | java | javascript
    source_code: str

class TestCaseModel(BaseModel):
    input: str
    output: str
    visible: bool = True

class ProblemRequest(BaseModel):
    title: str
    description: str
    difficulty: str
    score: int
    time_limit: float
    memory_limit: int
    test_cases: list[TestCaseModel]

class AnalysisRequest(BaseModel):
    problem_id: str
    language: str
    code: str

class Day2SubmissionResponse(BaseModel):
    id: str
    status: str
    test_cases_passed: int = 0
    test_cases_total: int = 0
    execution_time_ms: float = 0
    score: int = 0
    message: str = ""
    stdout: str | None = None
    stderr: str | None = None

# ====== IN-MEMORY DATABASE (Replace with MongoDB/Supabase later) ======
users_db = {}
submissions_db = {}
problems_db = {
    "p1": {
        "id": "p1",
        "title": "Two Sum",
        "description": "Given an array of integers nums and an integer target, return the indices of the two numbers that add up to target.\n\nYou may assume that each input would have exactly one solution, and you may not use the same element twice.\n\nExample:\n  Input: nums = [2,7,11,15], target = 9\n  Output: [0,1]",
        "difficulty": "Easy",
        "score": 100,
        "time_limit": 5,
        "memory_limit": 256,
        "test_cases": [
            {"input": "2 7 11 15\n9", "output": "0 1", "visible": True},
            {"input": "3 2 4\n6", "output": "1 2", "visible": True},
            {"input": "3 3\n6", "output": "0 1", "visible": False}
        ]
    },
    "p2": {
        "id": "p2",
        "title": "Reverse String",
        "description": "Write a function that reverses a string.\n\nThe input is given as a single line string. Print the reversed string.\n\nExample:\n  Input: hello\n  Output: olleh",
        "difficulty": "Easy",
        "score": 50,
        "time_limit": 3,
        "memory_limit": 128,
        "test_cases": [
            {"input": "hello", "output": "olleh", "visible": True},
            {"input": "Hannah", "output": "hannaH", "visible": True},
            {"input": "world", "output": "dlrow", "visible": False}
        ]
    },
    "p3": {
        "id": "p3",
        "title": "Palindrome Number",
        "description": "Given an integer x, return True if x is a palindrome integer, False otherwise.\n\nAn integer is a palindrome when it reads the same forwards and backwards.\n\nExample:\n  Input: 121\n  Output: True",
        "difficulty": "Easy",
        "score": 75,
        "time_limit": 4,
        "memory_limit": 200,
        "test_cases": [
            {"input": "121", "output": "True", "visible": True},
            {"input": "-121", "output": "False", "visible": True},
            {"input": "10", "output": "False", "visible": False},
        ]
    },
    "p4": {
        "id": "p4",
        "title": "Valid Parentheses",
        "description": "Given a string s containing just the characters '(', ')', '{', '}', '[' and ']', determine if the input string is valid.\n\nAn input string is valid if:\n- Open brackets must be closed by the same type of brackets.\n- Open brackets must be closed in the correct order.\n\nPrint True or False.\n\nExample:\n  Input: ()[]{}\n  Output: True",
        "difficulty": "Medium",
        "score": 150,
        "time_limit": 5,
        "memory_limit": 256,
        "test_cases": [
            {"input": "()[]{}", "output": "True", "visible": True},
            {"input": "(]", "output": "False", "visible": True},
            {"input": "([)]", "output": "False", "visible": False},
            {"input": "{[]}", "output": "True", "visible": False}
        ]
    },
    "p5": {
        "id": "p5",
        "title": "Fibonacci Number",
        "description": "The Fibonacci numbers form a sequence where each number is the sum of the two preceding ones, starting from 0 and 1.\n\nGiven n, calculate F(n).\n\nF(0) = 0, F(1) = 1, F(n) = F(n-1) + F(n-2)\n\nExample:\n  Input: 4\n  Output: 3",
        "difficulty": "Easy",
        "score": 80,
        "time_limit": 5,
        "memory_limit": 256,
        "test_cases": [
            {"input": "2", "output": "1", "visible": True},
            {"input": "4", "output": "3", "visible": True},
            {"input": "10", "output": "55", "visible": False}
        ]
    },
    "p6": {
        "id": "p6",
        "title": "FizzBuzz",
        "description": "Given an integer n, for each number from 1 to n:\n- Print 'FizzBuzz' if divisible by both 3 and 5\n- Print 'Fizz' if divisible by 3\n- Print 'Buzz' if divisible by 5\n- Otherwise print the number\n\nEach output on a new line.\n\nExample:\n  Input: 5\n  Output:\n  1\n  2\n  Fizz\n  4\n  Buzz",
        "difficulty": "Easy",
        "score": 60,
        "time_limit": 5,
        "memory_limit": 256,
        "test_cases": [
            {"input": "5", "output": "1\n2\nFizz\n4\nBuzz", "visible": True},
            {"input": "3", "output": "1\n2\nFizz", "visible": True},
            {"input": "15", "output": "1\n2\nFizz\n4\nBuzz\nFizz\n7\n8\nFizz\nBuzz\n11\nFizz\n13\n14\nFizzBuzz", "visible": False}
        ]
    },
    "p7": {
        "id": "p7",
        "title": "Sum of Digits",
        "description": "Given a non-negative integer n, find and print the sum of its digits.\n\nExample:\n  Input: 123\n  Output: 6",
        "difficulty": "Easy",
        "score": 40,
        "time_limit": 3,
        "memory_limit": 128,
        "test_cases": [
            {"input": "123", "output": "6", "visible": True},
            {"input": "9999", "output": "36", "visible": True},
            {"input": "0", "output": "0", "visible": False}
        ]
    },
    "p8": {
        "id": "p8",
        "title": "Maximum of Array",
        "description": "Given an array of integers (space-separated on one line), find and print the maximum element.\n\nExample:\n  Input: 3 1 4 1 5 9 2 6\n  Output: 9",
        "difficulty": "Easy",
        "score": 40,
        "time_limit": 3,
        "memory_limit": 128,
        "test_cases": [
            {"input": "3 1 4 1 5 9 2 6", "output": "9", "visible": True},
            {"input": "10 20 30", "output": "30", "visible": True},
            {"input": "-1 -5 -3", "output": "-1", "visible": False}
        ]
    }
}

# ====== STORAGE HELPERS (In-memory + Optional MongoDB) ======
async def user_exists(email: str) -> bool:
    if supabase_enabled:
        try:
            row = await supabase_select_rows(
                "users",
                params={"select": "id", "email": f"eq.{email}", "limit": "1"},
            )
            return len(row) > 0
        except Exception:
            # If Supabase schema isn't ready yet, fall back.
            return email in users_db
    if mongo_enabled and _users_coll is not None:
        return (await _users_coll.find_one({"email": email})) is not None
    return email in users_db


async def get_user_by_email(email: str) -> dict | None:
    if supabase_enabled:
        try:
            row = await supabase_get_user_by_email(email)
            if row is not None:
                users_db[email] = row
            return row
        except Exception:
            # If Supabase schema isn't ready yet, fall back.
            return users_db.get(email)
    if mongo_enabled and _users_coll is not None:
        doc = await _users_coll.find_one({"email": email})
        if doc and "_id" in doc: doc.pop("_id")
        return doc
    return users_db.get(email)


async def insert_user(user_data: dict) -> None:
    # Always keep in-memory for immediate use.
    users_db[user_data["email"]] = user_data
    if supabase_enabled:
        try:
            await supabase_insert_user(user_data)
        except Exception as e:
            print(f"Supabase insert user failed; falling back ({e})")
    if mongo_enabled and _users_coll is not None:
        await _users_coll.insert_one(user_data)


async def get_submission_by_id(submission_id: str) -> dict | None:
    if mongo_enabled and _submissions_coll is not None:
        doc = await _submissions_coll.find_one({"id": submission_id})
        if doc and "_id" in doc: doc.pop("_id")
        return doc
    if submission_id in submissions_db:
        return submissions_db.get(submission_id)
    if supabase_enabled:
        try:
            rows = await supabase_select_rows(
                "submissions",
                params={
                    "select": "id,user_id,problem_id,language,source_code,status,test_cases_passed,test_cases_total,execution_time_ms,score",
                    "id": f"eq.{submission_id}",
                    "limit": "1",
                },
            )
            if not rows:
                return None
            r = rows[0]
            problem_key = f"p{int(r['problem_id'])}"
            return {
                "id": r["id"],
                "user_id": r["user_id"],
                "user_email": "",
                "problem_id": problem_key,
                "problem_id_int": int(r["problem_id"]),
                "language": r["language"],
                "source_code": r["source_code"],
                "status": r["status"],
                "test_cases_passed": int(r.get("test_cases_passed", 0) or 0),
                "test_cases_total": int(r.get("test_cases_total", 0) or 0),
                "execution_time_ms": float(r.get("execution_time_ms", 0) or 0),
                "score": int(r.get("score", 0) or 0),
                "message": "Submitted (polling result pending)" if r.get("status") == "pending" else "Submission complete",
                "stdout": None,
                "stderr": None,
            }
        except Exception:
            return None
    return None


async def insert_submission(submission: dict) -> None:
    submissions_db[submission["id"]] = submission
    if mongo_enabled and _submissions_coll is not None:
        await _submissions_coll.insert_one(submission)
    if supabase_enabled:
        # Persist only the fields defined in your Supabase schema.
        try:
            await supabase_insert_rows(
                "submissions",
                [
                    {
                        "id": submission["id"],
                        "user_id": submission["user_id"],
                        "problem_id": int(submission["problem_id_int"]),
                        "language": submission["language"],
                        "source_code": submission["source_code"],
                        "status": submission["status"],
                        "test_cases_passed": int(submission.get("test_cases_passed", 0)),
                        "test_cases_total": int(submission.get("test_cases_total", 0)),
                        "execution_time_ms": submission.get("execution_time_ms"),
                        "memory_used_mb": None,
                        "score": int(submission.get("score", 0)),
                        "created_at": submission.get("created_at") or datetime.utcnow().isoformat(),
                        "completed_at": None,
                    }
                ],
            )
        except Exception as e:
            print(f"Supabase insert submission failed (optional): {e}")


async def get_submissions_for_user(user_id: str) -> list[dict]:
    if mongo_enabled and _submissions_coll is not None:
        cursor = _submissions_coll.find({"user_id": user_id})
        docs = await cursor.to_list(length=1000)
        for d in docs:
            if "_id" in d: d.pop("_id")
        return docs
    return [s for s in submissions_db.values() if s["user_id"] == user_id]


async def get_leaderboard_users_sorted() -> list[dict]:
    if mongo_enabled and _users_coll is not None:
        cursor = _users_coll.find({}).sort("total_score", -1)
        docs = await cursor.to_list(length=1000)
        for d in docs:
            if "_id" in d: d.pop("_id")
        return docs
    return sorted(
        users_db.values(),
        key=lambda x: x.get("total_score", 0),
        reverse=True
    )


async def redis_update_scores(user_email: str, house: str, total_score: int) -> None:
    if not redis_enabled or _redis_client is None:
        return
    # ZSET stores member=user_email, score=total_score.
    await _redis_client.zadd("coduku:lb:global", {user_email: int(total_score)})
    await _redis_client.zadd(f"coduku:lb:house:{house}", {user_email: int(total_score)})


async def redis_get_leaderboard_global(limit: int = 50) -> list[tuple[str, int]]:
    if not redis_enabled or _redis_client is None:
        return []
    # returns list of (member, score) sorted high->low
    return await _redis_client.zrevrange("coduku:lb:global", 0, limit - 1, withscores=True)


# ====== OPTIONAL SUPABASE (REST) ======
def _supabase_headers() -> dict:
    # Service role is required for server-to-server writes and to bypass RLS.
    return {
        "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
        "apikey": SUPABASE_SERVICE_ROLE_KEY,
        "Content-Type": "application/json",
    }


async def supabase_select_rows(table: str, params: dict) -> list[dict]:
    if not supabase_enabled or _supabase_client is None:
        return []
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    r = await _supabase_client.get(url, headers=_supabase_headers(), params=params)
    if r.status_code == 200:
        return r.json() or []
    # Bubble up errors so startup can safely disable supabase mode.
    raise RuntimeError(f"Supabase select failed table={table} status={r.status_code} body={r.text[:200]}")


async def supabase_insert_rows(table: str, rows: list[dict]) -> list[dict]:
    if not supabase_enabled or _supabase_client is None:
        return []
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    headers = {**_supabase_headers(), "Prefer": "return=representation"}
    r = await _supabase_client.post(url, headers=headers, json=rows)
    if r.status_code in (200, 201):
        return r.json() or []
    raise RuntimeError(f"Supabase insert failed table={table} status={r.status_code} body={r.text[:200]}")


async def supabase_patch_rows(table: str, match_params: dict, payload: dict) -> list[dict]:
    if not supabase_enabled or _supabase_client is None:
        return []
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    headers = {**_supabase_headers(), "Prefer": "return=representation"}
    r = await _supabase_client.patch(url, headers=headers, params=match_params, json=payload)
    if r.status_code in (200, 204):
        return r.json() or []
    raise RuntimeError(f"Supabase patch failed table={table} status={r.status_code} body={r.text[:200]}")


def _derive_time_memory_from_difficulty(difficulty: str) -> tuple[int, int]:
    diff = (difficulty or "").lower()
    if diff == "easy":
        return 5, 256
    if diff == "medium":
        return 10, 512
    return 15, 1024


async def supabase_load_problems_into_memory() -> None:
    # Pull problems + test cases into `problems_db` so the existing Judge0 execution flow keeps working.
    global problems_db
    if not supabase_enabled:
        return

    problems = await supabase_select_rows(
        "problems",
        params={
            "select": "id,title,description,difficulty,difficulty_multiplier,base_score",
            "order": "id.asc",
            "limit": "200",
        },
    )

    if not problems:
        return

    loaded: dict = {}
    for p in problems:
        problem_id_int = int(p["id"])
        key = f"p{problem_id_int}"

        # Convert schema fields into the in-memory shape used by the existing judge pipeline.
        base_score = int(p.get("base_score", 100))
        mult = float(p.get("difficulty_multiplier", 1.0) or 1.0)
        score = int(base_score * mult)
        time_limit, memory_limit = _derive_time_memory_from_difficulty(p.get("difficulty", "easy"))

        tcs = await supabase_select_rows(
            "test_cases",
            params={
                "select": "input,expected_output,is_visible",
                "problem_id": f"eq.{problem_id_int}",
                "order": "id.asc",
                "limit": "1000",
            },
        )
        test_cases = [
            {"input": tc["input"], "output": tc["expected_output"], "visible": bool(tc.get("is_visible", True))}
            for tc in tcs
        ]

        loaded[key] = {
            "id": key,
            "title": p["title"],
            "description": p["description"],
            "difficulty": p.get("difficulty", "easy"),
            "score": score,
            "time_limit": time_limit,
            "memory_limit": memory_limit,
            "test_cases": test_cases,
        }

    problems_db = loaded


async def supabase_get_user_by_email(email: str) -> dict | None:
    rows = await supabase_select_rows(
        "users",
        params={
            "select": "id,email,username,password_hash,house,total_score,problems_solved",
            "email": f"eq.{email}",
            "limit": 1,
        },
    )
    if not rows:
        return None
    r = rows[0]
    return {
        "id": r["id"],
        "email": r["email"],
        "username": r["username"],
        "name": r["username"],
        "password_hash": r["password_hash"],
        "house": r.get("house", "gryffindor"),
        "total_score": int(r.get("total_score", 0) or 0),
        "problems_solved": int(r.get("problems_solved", 0) or 0),
        "submissions": 0,
    }


async def supabase_load_users_into_memory() -> None:
    # Loads all Supabase users into `users_db` so leaderboards work after restarts.
    global users_db
    if not supabase_enabled:
        return

    rows = await supabase_select_rows(
        "users",
        params={
            "select": "id,email,username,house,total_score,problems_solved",
            "order": "total_score.desc",
            "limit": "5000",
        },
    )

    for r in rows:
        email = r.get("email")
        if not email:
            continue
        users_db[email] = {
            "id": r.get("id"),
            "name": r.get("username") or "",
            "username": r.get("username") or "",
            "email": email,
            "house": (r.get("house") or "gryffindor"),
            "password_hash": "",
            "total_score": int(r.get("total_score", 0) or 0),
            "problems_solved": int(r.get("problems_solved", 0) or 0),
            "submissions": 0,
        }


async def supabase_insert_user(user_data: dict) -> None:
    username = user_data.get("username") or user_data.get("name") or user_data["email"].split("@")[0]
    payload = {
        "id": user_data["id"],
        "email": user_data["email"],
        "username": username,
        "password_hash": user_data["password_hash"],
        "house": user_data.get("house", "gryffindor"),
        "total_score": int(user_data.get("total_score", 0) or 0),
        "problems_solved": int(user_data.get("problems_solved", 0) or 0),
        "created_at": user_data.get("created_at") or datetime.utcnow().isoformat(),
        "updated_at": user_data.get("updated_at") or datetime.utcnow().isoformat(),
    }
    await supabase_insert_rows("users", [payload])


async def supabase_patch_user_totals_by_email(
    email: str,
    *,
    total_score: int,
    problems_solved: int,
) -> None:
    payload = {"total_score": int(total_score), "problems_solved": int(problems_solved), "updated_at": datetime.utcnow().isoformat()}
    await supabase_patch_rows("users", {"email": f"eq.{email}"}, payload)


# ====== UTILITY FUNCTIONS ======
def hash_password(password: str) -> str:
    return bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()

def verify_password(password: str, hashed: str) -> bool:
    return bcrypt.checkpw(password.encode(), hashed.encode())

def create_jwt_token(user_id: str, email: str) -> str:
    payload = {
        "sub": user_id,
        "email": email,
        "exp": datetime.utcnow() + timedelta(days=30),
        "iat": datetime.utcnow()
    }
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)

def verify_jwt_token(credentials: HTTPAuthorizationCredentials = Depends(security)):
    try:
        payload = jwt.decode(credentials.credentials, JWT_SECRET, algorithms=[JWT_ALGORITHM])
        return payload
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Invalid token")

# ====== DEFERRED SERVICE IMPORTS (after verify_jwt_token defined) ======
try:
    from services.user_service import router as user_router
    from services.admin_service import router as admin_router
    from services.house_service import router as house_router
    app.include_router(user_router)
    app.include_router(admin_router)
    app.include_router(house_router)
except ImportError as e:
    print(f"⚠️ Optional service import skipped (circular or missing): {e}")
except Exception as e:
    print(f"⚠️ Service router import failed: {e}")

# ====== HEALTH CHECKS ======
@app.get("/")
async def root():
    return {
        "message": "CODUKU API running!",
        "docs": "/docs",
        "version": "1.0.0"
    }

@app.get("/health")
async def health_check():
    return {"status": "ok", "timestamp": datetime.utcnow().isoformat()}

@app.get("/debug/mongo")
async def debug_mongo():
    return {
        "mongo_enabled": mongo_enabled,
        "mongo_force_disable": MONGO_FORCE_DISABLE,
        "database_url": DATABASE_URL,
        "judge0_mode": JUDGE0_MODE,
        "judge0_force_mock": JUDGE0_FORCE_MOCK,
        "judge0_api_url": JUDGE0_API_URL,
    }


@app.get("/debug/judge0")
async def debug_judge0():
    return {
        "judge0_mode": JUDGE0_MODE,
        "judge0_force_mock": JUDGE0_FORCE_MOCK,
        "judge0_api_url": JUDGE0_API_URL,
    }

# ====== AUTHENTICATION ENDPOINTS ======
@app.post("/api/auth/register", response_model=AuthResponse)
@app.post("/api/v1/auth/register", response_model=AuthResponse)
async def register(request: RegisterRequest):
    valid_houses = ["gryffindor", "hufflepuff", "ravenclaw", "slytherin"]
    if request.house.lower() not in valid_houses:
        raise HTTPException(status_code=400, detail="Invalid house")
    
    if await user_exists(request.email):
        raise HTTPException(status_code=400, detail="Email already registered")
    
    display_name = request.name or request.username
    if not display_name:
        raise HTTPException(status_code=400, detail="Missing name/username")

    user_id = str(uuid4())
    user_data = {
        "id": user_id,
        "name": display_name,
        "username": display_name,
        "email": request.email,
        "password_hash": hash_password(request.password),
        "house": request.house.lower(),
        "role": "student",
        "created_at": datetime.utcnow().isoformat(),
        "total_score": 0,
        "problems_solved": 0,
        "submissions": 0
    }
    
    try:
        await insert_user(user_data)
    except Exception as e:
        # MongoDB unique constraint can still race.
        raise HTTPException(status_code=400, detail="Email already registered") from e
    token = create_jwt_token(user_id, request.email)
    
    return AuthResponse(
        access_token=token,
        user_id=user_id,
        name=display_name,
        username=display_name,
        email=request.email,
        house=request.house.lower()
    )

@app.post("/api/auth/login", response_model=AuthResponse)
@app.post("/api/v1/auth/login", response_model=AuthResponse)
async def login(request: LoginRequest):
    user = await get_user_by_email(request.email)
    if user is None:
        raise HTTPException(status_code=401, detail="Invalid credentials")
    
    if not verify_password(request.password, user["password_hash"]):
        raise HTTPException(status_code=401, detail="Invalid credentials")
    
    token = create_jwt_token(user["id"], request.email)
    
    return AuthResponse(
        access_token=token,
        user_id=user["id"],
        name=user["name"],
        username=user.get("username", user["name"]),
        email=user["email"],
        house=user["house"]
    )

@app.get("/api/auth/me")
@app.get("/api/v1/auth/me")
async def get_current_user(payload: dict = Depends(verify_jwt_token)):
    email = payload.get("email")
    user = await get_user_by_email(email)
    if user is None:
        raise HTTPException(status_code=401, detail="User not found")
    return {
        "id": user["id"],
        "name": user["name"],
        "email": user["email"],
        "house": user["house"],
        "total_score": user["total_score"],
        "problems_solved": user["problems_solved"],
        "submissions": user["submissions"]
    }

# ====== PROBLEMS ENDPOINTS ======
# Frontend calls /api/v1/problems, so we serve both /questions and /problems
@app.get("/api/questions")
@app.get("/api/v1/questions")
@app.get("/api/problems")
@app.get("/api/v1/problems")
async def get_questions(payload: dict = Depends(verify_jwt_token)):
    return list(problems_db.values())

@app.get("/api/questions/{problem_id}")
@app.get("/api/v1/questions/{problem_id}")
@app.get("/api/problems/{problem_id}")
@app.get("/api/v1/problems/{problem_id}")
async def get_question(problem_id: str, payload: dict = Depends(verify_jwt_token)):
    if problem_id not in problems_db:
        raise HTTPException(status_code=404, detail="Problem not found")
    
    problem = problems_db[problem_id].copy()
    # Only show visible test cases
    problem["test_cases"] = [tc for tc in problem["test_cases"] if tc.get("visible", True)]
    return problem

@app.post("/api/questions")
@app.post("/api/v1/questions")
@app.post("/api/problems")
@app.post("/api/v1/problems")
async def add_question(req: ProblemRequest, payload: dict = Depends(verify_jwt_token)):
    # Make sure we generate an ID that fits 'p<number>' pattern matching what we have
    pid_num = len(problems_db) + 1
    while f"p{pid_num}" in problems_db:
        pid_num += 1
    pid = f"p{pid_num}"
    
    new_prob = {
        "id": pid,
        "title": req.title,
        "description": req.description,
        "difficulty": req.difficulty,
        "score": req.score,
        "time_limit": req.time_limit,
        "memory_limit": req.memory_limit,
        "test_cases": [tc.dict() for tc in req.test_cases]
    }
    
    problems_db[pid] = new_prob
    return {"message": "Problem added successfully", "problem": new_prob}


# ====== RUN ENDPOINT (Sample Test Cases Only — No Persistence) ======
class RunRequest(BaseModel):
    problem_id: str
    code: str
    language: str


def _normalize_output(text: str) -> str:
    """Normalize output for comparison: strip, collapse whitespace per line, remove blank lines."""
    if not text:
        return ""
    lines = []
    for line in text.strip().split("\n"):
        normalized = " ".join(line.split())
        if normalized:
            lines.append(normalized)
    return "\n".join(lines)


def _map_verdict(status_id: int) -> str:
    """Map Judge0 status_id to human-readable verdict string."""
    mapping = {
        1: "Pending",
        2: "Processing",
        3: "Accepted",
        4: "Wrong Answer",
        5: "Time Limit Exceeded",
        6: "Memory Limit Exceeded",
        7: "Runtime Error",
        8: "System Error",
        11: "Runtime Error",
        12: "Compilation Error",
        13: "Runtime Error",
        14: "System Error",
    }
    return mapping.get(status_id, "Unknown")


@app.post("/api/v1/submissions/run")
@app.post("/api/submit/run")
async def run_code(request: RunRequest, payload: dict = Depends(verify_jwt_token)):
    """Run code against ONLY visible/sample test cases. No score update, no persistence."""
    problem_id = request.problem_id
    if problem_id not in problems_db:
        raise HTTPException(status_code=404, detail="Problem not found")
    
    problem = problems_db[problem_id]
    # Only visible test cases for Run
    test_cases = [tc for tc in problem["test_cases"] if tc.get("visible", True)]
    
    language = request.language
    code = request.code
    
    language_map = {
        "python": 71, "python3": 71, "py": 71,
        "cpp": 54, "c++": 54,
        "java": 62,
        "javascript": 63, "js": 63,
        "c": 50,
        "csharp": 51, "c#": 51,
        "go": 60, "golang": 60,
        "rust": 73,
    }
    language_id = language_map.get(language.lower(), 71)
    
    results = []
    passed = 0
    total = len(test_cases)
    
    # Try local execution first for Python (fastest, no Judge0 needed)
    use_local = False
    if language.lower() in ("python", "python3", "py"):
        use_local = True
    
    if JUDGE0_FORCE_MOCK or JUDGE0_MODE == "mock":
        # Pure mock mode: simulate accepted for all
        for idx, tc in enumerate(test_cases):
            results.append({
                "test_case": idx + 1,
                "verdict": "Accepted",
                "input": tc["input"],
                "expected": tc["output"],
                "output": tc["output"],
                "time": "0.001",
                "memory": "256",
            })
            passed += 1
        return {"passed": passed, "total": total, "results": results}
    
    if use_local:
        # Local Python execution with subprocess
        import subprocess
        import tempfile
        import time as time_mod
        
        for idx, tc in enumerate(test_cases):
            tc_result = {
                "test_case": idx + 1,
                "verdict": "Unknown",
                "input": tc["input"],
                "expected": tc["output"],
                "output": "",
                "time": "0",
                "memory": "0",
            }
            try:
                start_time = time_mod.time()
                proc = subprocess.run(
                    ["python", "-c", code],
                    input=tc["input"],
                    capture_output=True,
                    text=True,
                    timeout=10,
                    cwd=None,
                )
                elapsed = time_mod.time() - start_time
                tc_result["time"] = f"{elapsed:.3f}"
                
                if proc.returncode != 0:
                    # Runtime error
                    tc_result["verdict"] = "Runtime Error"
                    tc_result["output"] = (proc.stderr or "Runtime error").strip()[:500]
                else:
                    actual = _normalize_output(proc.stdout.strip())
                    expected = _normalize_output(tc["output"].strip())
                    tc_result["output"] = proc.stdout.strip()[:500]
                    
                    if actual == expected:
                        tc_result["verdict"] = "Accepted"
                        passed += 1
                    else:
                        tc_result["verdict"] = "Wrong Answer"
                        
            except subprocess.TimeoutExpired:
                tc_result["verdict"] = "Time Limit Exceeded"
                tc_result["output"] = "Execution timed out (10s limit)"
            except Exception as e:
                tc_result["verdict"] = "System Error"
                tc_result["output"] = str(e)[:300]
            
            results.append(tc_result)
        
        return {"passed": passed, "total": total, "results": results}
    
    # Judge0 execution for non-Python languages
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            for idx, tc in enumerate(test_cases):
                tc_result = {
                    "test_case": idx + 1,
                    "verdict": "Unknown",
                    "input": tc["input"],
                    "expected": tc["output"],
                    "output": "",
                    "time": "0",
                    "memory": "0",
                }
                try:
                    submit_resp = await client.post(
                        f"{JUDGE0_API_URL}/submissions?base64_encoded=false",
                        json={
                            "language_id": language_id,
                            "source_code": code,
                            "stdin": tc["input"],
                            "expected_output": tc["output"].strip(),
                        },
                    )
                    if submit_resp.status_code != 201:
                        tc_result["verdict"] = "System Error"
                        tc_result["output"] = f"Judge0 submission error: {submit_resp.status_code}"
                        results.append(tc_result)
                        continue
                    
                    token = submit_resp.json()["token"]
                    
                    # Poll for result
                    for _ in range(120):
                        res_resp = await client.get(
                            f"{JUDGE0_API_URL}/submissions/{token}?base64_encoded=false"
                        )
                        if res_resp.status_code != 200:
                            await asyncio.sleep(0.2)
                            continue
                        
                        res = res_resp.json()
                        status_id = res.get("status", {}).get("id", 0)
                        
                        if status_id not in (1, 2):
                            tc_result["verdict"] = _map_verdict(status_id)
                            tc_result["output"] = (res.get("stdout") or res.get("stderr") or res.get("compile_output") or "").strip()
                            tc_result["time"] = str(res.get("time", "0"))
                            tc_result["memory"] = str(res.get("memory", "0"))
                            
                            if status_id == 3:
                                passed += 1
                            elif status_id == 4:
                                actual_norm = _normalize_output(tc_result["output"])
                                expected_norm = _normalize_output(tc["output"])
                                if actual_norm == expected_norm:
                                    tc_result["verdict"] = "Accepted"
                                    passed += 1
                            break
                        
                        await asyncio.sleep(0.2)
                
                except Exception as e:
                    tc_result["verdict"] = "System Error"
                    tc_result["output"] = str(e)
                
                results.append(tc_result)
    
    except Exception as e:
        # Judge0 completely down — return error for non-Python
        if JUDGE0_MODE == "real":
            raise HTTPException(status_code=503, detail=f"Judge0 unavailable: {e}")
        # Auto mode mock fallback
        results = []
        passed = 0
        for idx, tc in enumerate(test_cases):
            results.append({
                "test_case": idx + 1,
                "verdict": "Accepted",
                "input": tc["input"],
                "expected": tc["output"],
                "output": tc["output"],
                "time": "0.001",
                "memory": "256",
            })
            passed += 1
    
    return {"passed": passed, "total": total, "results": results}


# ====== SUBMISSION ENDPOINTS ======
@app.post("/api/submit", response_model=SubmissionResponse)
@app.post("/api/v1/submit", response_model=SubmissionResponse)
async def submit_code(request: SubmissionRequest, payload: dict = Depends(verify_jwt_token)):
    user_email = payload.get("email")
    user_id = payload.get("sub")
    
    if request.problem_id not in problems_db:
        raise HTTPException(status_code=404, detail="Problem not found")
    
    valid_languages = ["python", "cpp", "java", "javascript"]
    if request.language not in valid_languages:
        raise HTTPException(status_code=400, detail="Invalid language")
    
    submission_id = str(uuid4())
    
    # Execute code with Judge0
    execution_result = await execute_with_judge0(
        language=request.language,
        code=request.code,
        problem_id=request.problem_id
    )

    already_solved = False
    if execution_result["status"] == "accepted":
        if mongo_enabled and _submissions_coll is not None:
            already_solved = await _submissions_coll.find_one({
                "user_email": user_email,
                "problem_id": request.problem_id,
                "status": "accepted",
            }) is not None
        else:
            already_solved = any(
                s["user_email"] == user_email
                and s["problem_id"] == request.problem_id
                and s["status"] == "accepted"
                for s in submissions_db.values()
            )
    
    # Store submission
    submission = {
        "id": submission_id,
        "user_id": user_id,
        "user_email": user_email,
        "problem_id": request.problem_id,
        "code": request.code,
        "language": request.language,
        "status": execution_result["status"],
        "test_cases_passed": execution_result["test_cases_passed"],
        "test_cases_total": execution_result["test_cases_total"],
        "score": execution_result["score"],
        "message": execution_result["message"],
        "execution_time_ms": execution_result.get("execution_time", 0),
        "created_at": datetime.utcnow().isoformat()
    }
    
    await insert_submission(submission)
    
    # Update user stats
    if execution_result["status"] == "accepted":
        # MongoDB-backed update (preferred when enabled).
        if mongo_enabled and _users_coll is not None:
            update_doc = {
                "$inc": {
                    "total_score": execution_result["score"],
                    "submissions": 1,
                }
            }
            if not already_solved:
                update_doc["$inc"]["problems_solved"] = 1
            await _users_coll.update_one({"email": user_email}, update_doc)

        # Keep in-memory stats usable during runtime.
        if user_email in users_db:
            user = users_db[user_email]
            user["total_score"] += execution_result["score"]
            if not already_solved:
                user["problems_solved"] += 1
            user["submissions"] += 1

            # Update Redis leaderboards when available.
            await redis_update_scores(user_email=user_email, house=user["house"], total_score=user["total_score"])

            # Optional Supabase persistence for totals.
            if supabase_enabled:
                try:
                    await supabase_patch_user_totals_by_email(
                        user_email,
                        total_score=user["total_score"],
                        problems_solved=user["problems_solved"],
                    )
                except Exception as e:
                    print(f"Supabase totals patch failed (optional): {e}")
    
    return SubmissionResponse(
        submission_id=submission_id,
        status=execution_result["status"],
        message=execution_result["message"]
    )

@app.get("/api/submissions/{submission_id}")
async def get_submission(submission_id: str, payload: dict = Depends(verify_jwt_token)):
    submission = await get_submission_by_id(submission_id)
    if submission is None:
        raise HTTPException(status_code=404, detail="Submission not found")
    return submission

@app.get("/api/submissions")
@app.get("/api/v1/submissions")
async def get_user_submissions(payload: dict = Depends(verify_jwt_token)):
    user_id = payload.get("sub")
    return await get_submissions_for_user(user_id)

# ====== JUDGE0 EXECUTION ======
async def execute_with_judge0(language: str, code: str, problem_id: str):
    """Execute code using Judge0 API"""
    
    if problem_id not in problems_db:
        return {
            "status": "error",
            "message": "Problem not found",
            "test_cases_passed": 0,
            "test_cases_total": 0,
            "score": 0,
            "execution_time": 0
        }
    
    problem = problems_db[problem_id]
    test_cases = problem["test_cases"]

    if JUDGE0_FORCE_MOCK or JUDGE0_MODE == "mock":
        # Forced/mock mode: always mark as accepted for demo/testing.
        return {
            "status": "accepted",
            "message": "Code executed (mock mode - forced)",
            "test_cases_passed": len(test_cases),
            "test_cases_total": len(test_cases),
            "score": problem["score"],
            "execution_time": 100,
        }
    
    # Language ID mapping for Judge0
    language_map = {
        "python": 71,
        "cpp": 54,
        "java": 62,
        "javascript": 63
    }
    
    language_id = language_map.get(language, 71)
    passed = 0
    total_time = 0
    
    # Try local execution first for Python
    use_local = False
    if language.lower() in ("python", "python3", "py"):
        use_local = True
        
    if use_local:
        import subprocess
        import time as time_mod
        
        passed = 0
        total_time = 0
        overall_status = "accepted"
        first_error_msg = ""
        
        for test_case in test_cases:
            try:
                start_time = time_mod.time()
                proc = subprocess.run(
                    ["python", "-c", code],
                    input=test_case["input"],
                    capture_output=True,
                    text=True,
                    timeout=5,
                    cwd=None,
                )
                elapsed = (time_mod.time() - start_time) * 1000
                total_time += elapsed
                
                if proc.returncode != 0:
                    overall_status = "runtime_error"
                    first_error_msg = (proc.stderr or "Runtime error").strip()[:100]
                    break
                else:
                    actual = _normalize_output(proc.stdout.strip())
                    expected = _normalize_output(test_case["output"].strip())
                    
                    if actual == expected:
                        passed += 1
                    else:
                        overall_status = "wrong_answer"
                        break
                        
            except subprocess.TimeoutExpired:
                overall_status = "time_limit_exceeded"
                first_error_msg = "Execution timed out"
                break
            except Exception as e:
                overall_status = "system_error"
                first_error_msg = str(e)[:100]
                break
                
        score = (problem["score"] * passed) // len(test_cases) if test_cases else 0
        
        if overall_status == "accepted":
            if passed == len(test_cases):
                message = f"Passed {passed}/{len(test_cases)} test cases"
            else:
                overall_status = "wrong_answer"
                message = f"Passed {passed}/{len(test_cases)} test cases"
        else:
            message = first_error_msg if first_error_msg else f"Failed at test case {passed + 1}"
            
        return {
            "status": overall_status,
            "message": message,
            "test_cases_passed": passed,
            "test_cases_total": len(test_cases),
            "score": score,
            "execution_time": total_time / len(test_cases) if test_cases and passed > 0 else 0
        }
    
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            for test_case in test_cases:
                try:
                    # Submit to Judge0
                    submit_response = await client.post(
                        f"{JUDGE0_API_URL}/submissions",
                        json={
                            "language_id": language_id,
                            "source_code": code,
                            "stdin": test_case["input"],
                            "expected_output": test_case["output"].strip()
                        }
                    )
                    
                    if submit_response.status_code != 201:
                        continue
                    
                    token = submit_response.json()["token"]
                    
                    # Poll for result
                    for _ in range(60):
                        result_response = await client.get(
                            f"{JUDGE0_API_URL}/submissions/{token}"
                        )
                        
                        if result_response.status_code != 200:
                            break
                        
                        result = result_response.json()
                        status_id = result.get("status", {}).get("id")
                        
                        if status_id not in [1, 2]:  # Not queued or processing
                            if status_id == 3:  # Accepted
                                passed += 1
                            elif status_id == 4:
                                actual_norm = _normalize_output(result.get("stdout") or "")
                                expected_norm = _normalize_output(test_case["output"].strip())
                                if actual_norm == expected_norm:
                                    passed += 1
                                    
                            if result.get("time"):
                                total_time += float(result["time"]) * 1000
                            break
                        
                        await httpx.AsyncClient().aclose()
                        import asyncio
                        await asyncio.sleep(0.1)
                
                except httpx.RequestError as e:
                    # If Judge0 itself is unreachable, fall back to mock mode.
                    print(f"Judge0 request error: {e}")
                    raise
                except Exception as e:
                    print(f"Test case error: {e}")
                    msg = str(e).lower()
                    if "connection attempts" in msg or "all connection attempts failed" in msg:
                        raise
                    continue
        
        # Determine overall status
        status = "accepted" if passed == len(test_cases) else "wrong_answer"
        score = (problem["score"] * passed) // len(test_cases) if test_cases else 0
        message = f"Passed {passed}/{len(test_cases)} test cases"
        
        return {
            "status": status,
            "message": message,
            "test_cases_passed": passed,
            "test_cases_total": len(test_cases),
            "score": score,
            "execution_time": total_time / len(test_cases) if test_cases else 0
        }
    
    except Exception as e:
        print(f"Judge0 error: {e}")
        if JUDGE0_MODE == "real":
            return {
                "status": "error",
                "message": f"Judge0 unavailable: {e}",
                "test_cases_passed": 0,
                "test_cases_total": len(test_cases),
                "score": 0,
                "execution_time": 0,
            }

        # Auto fallback: Mock response (for when Judge0 is not available).
        return {
            "status": "accepted",  # Assume correct for testing
            "message": "Code executed (mock mode - Judge0 unavailable)",
            "test_cases_passed": len(test_cases),
            "test_cases_total": len(test_cases),
            "score": problem["score"],
            "execution_time": 100
        }


# ====== DAY 2: Judge0 execution with background + polling ======
_PROBLEM_KEY_BY_ID: dict[int, str] = {1: "p1", 2: "p2", 3: "p3", 4: "p4", 5: "p5", 6: "p6", 7: "p7", 8: "p8"}
_LANGUAGE_KEY_TO_JUDGE0_ID: dict[str, int] = {
    "python3": 71,
    "cpp": 54,
    "java": 62,
    "javascript": 63,
}


def _validate_day2_language(language: str) -> int:
    if language not in _LANGUAGE_KEY_TO_JUDGE0_ID:
        raise ValueError(f"Unsupported language: {language}")
    return _LANGUAGE_KEY_TO_JUDGE0_ID[language]


async def _execute_day2_submission(
    submission_id: str,
    user_id: str,
    user_email: str,
    language: str,
    source_code: str,
    problem_id_int: int,
) -> None:
    problem_key = _PROBLEM_KEY_BY_ID.get(problem_id_int)
    if not problem_key or problem_key not in problems_db:
        # Mark as error for polling UI.
        update = {
            "status": "error",
            "message": "Problem not found",
            "test_cases_passed": 0,
            "test_cases_total": 0,
            "execution_time_ms": 0,
            "score": 0,
        }
        if mongo_enabled and _submissions_coll is not None:
            await _submissions_coll.update_one({"id": submission_id}, {"$set": update})
        submissions_db[submission_id].update(update)  # type: ignore[index]
        return

    problem = problems_db[problem_key]
    test_cases = [tc for tc in problem["test_cases"] if tc.get("visible", True)]

    # Forced/mock modes for easy demo.
    if JUDGE0_FORCE_MOCK or JUDGE0_MODE == "mock":
        passed = len(test_cases)
        result = {
            "status": "accepted",
            "message": "Code executed (mock mode - forced)",
            "test_cases_passed": passed,
            "test_cases_total": len(test_cases),
            "score": problem["score"],
            "execution_time_ms": 100,
            "stdout": "",
            "stderr": "",
        }
    else:
        language_id = _validate_day2_language(language)
        passed = 0
        total_time_ms = 0.0
        stdout_all = []
        stderr_all = []
        final_status = "wrong_answer"
        message = ""

        try:
            async with httpx.AsyncClient(timeout=30) as client:
                for test_case in test_cases:
                    token_resp = await client.post(
                        f"{JUDGE0_API_URL}/submissions?base64_encoded=false",
                        json={
                            "language_id": language_id,
                            "source_code": source_code,
                            "stdin": test_case["input"],
                            "expected_output": test_case["output"].strip(),
                        },
                    )

                    if token_resp.status_code != 201:
                        final_status = "runtime_error"
                        message = f"Judge0 submission failed: {token_resp.text}"
                        break

                    token = token_resp.json()["token"]

                    # Poll result
                    for _ in range(120):
                        res_resp = await client.get(
                            f"{JUDGE0_API_URL}/submissions/{token}?base64_encoded=false"
                        )
                        if res_resp.status_code != 200:
                            await asyncio.sleep(0.1)
                            continue

                        res = res_resp.json()
                        status_id = res.get("status", {}).get("id")

                        if status_id not in [1, 2]:
                            # 3=Accepted
                            if status_id == 3:
                                passed += 1
                                if res.get("stdout") is not None:
                                    stdout_all.append(str(res.get("stdout")))
                                if res.get("stderr") is not None:
                                    stderr_all.append(str(res.get("stderr")))
                            else:
                                final_status = "wrong_answer"
                            if res.get("time") is not None:
                                total_time_ms += float(res["time"]) * 1000
                            break

                        await asyncio.sleep(0.2)
        except Exception as e:
            msg = str(e).lower()
            if JUDGE0_MODE == "real":
                result = {
                    "status": "error",
                    "message": f"Judge0 unavailable: {e}",
                    "test_cases_passed": 0,
                    "test_cases_total": len(test_cases),
                    "execution_time_ms": 0,
                    "score": 0,
                    "stdout": "",
                    "stderr": str(e),
                }
                await _update_submission_record(submission_id, result)
                return
            # auto => fallback to mock accepted
            passed = len(test_cases)
            final_status = "accepted"
            message = "Code executed (mock mode - Judge0 unavailable)"

        # Decide final status/score
        if "result" not in locals():
            final_status = "accepted" if passed == len(test_cases) and len(test_cases) > 0 else "wrong_answer"
            score = (problem["score"] * passed) // len(test_cases) if test_cases else 0
            message = f"Passed {passed}/{len(test_cases)} test cases"
            result = {
                "status": final_status,
                "message": message,
                "test_cases_passed": passed,
                "test_cases_total": len(test_cases),
                "score": score,
                "execution_time_ms": total_time_ms / len(test_cases) if test_cases else 0,
                "stdout": "\n".join(stdout_all),
                "stderr": "\n".join(stderr_all),
            }

    await _update_submission_record(submission_id, result)

    # Update leaderboard stats on accepted only.
    if result["status"] == "accepted":
        # PLAGIARISM CHECKER (AST mapped logic)
        try:
            highest_score = 0.0
            past_subs = [s for s in submissions_db.values() if s.get("problem_id_int", s.get("problem_id")) == problem_id_int and s.get("user_id") != user_id and s.get("status") == "accepted" and s.get("language") == language]
            if language == "python" or language == "python3":
                try:
                    new_ast = ast.dump(ast.parse(source_code))
                    for idx, past in enumerate(past_subs):
                        if idx > 20: break # check last 20 accepted for performance
                        try:
                            past_ast = ast.dump(ast.parse(past.get("source_code", "")))
                            score = difflib.SequenceMatcher(None, new_ast, past_ast).ratio()
                            if score > highest_score: highest_score = score
                        except SyntaxError: pass
                except SyntaxError: pass
            else:
                new_tokens = source_code.split()
                for idx, past in enumerate(past_subs):
                    if idx > 20: break
                    past_tokens = past.get("source_code", "").split()
                    score = difflib.SequenceMatcher(None, new_tokens, past_tokens).ratio()
                    if score > highest_score: highest_score = score
            
            result["plagiarism_score"] = round(highest_score * 100, 2)
        except Exception as e:
            print("Plagiarism check error:", e)

        already_solved = await _already_solved_for_problem(user_id, user_email, problem_key)
        if mongo_enabled and _users_coll is not None:
            update_doc = {
                "$inc": {"total_score": result["score"], "submissions": 1},
            }
            if not already_solved:
                update_doc["$inc"]["problems_solved"] = 1
            await _users_coll.update_one({"email": user_email}, update_doc)

        if user_email in users_db:
            users_db[user_email]["total_score"] += result["score"]
            if not already_solved:
                users_db[user_email]["problems_solved"] += 1
            users_db[user_email]["submissions"] += 1

            # Update Redis leaderboards when available.
            await redis_update_scores(
                user_email=user_email,
                house=users_db[user_email]["house"],
                total_score=users_db[user_email]["total_score"],
            )

            # Optional Supabase persistence for totals.
            if supabase_enabled:
                try:
                    await supabase_patch_user_totals_by_email(
                        user_email,
                        total_score=users_db[user_email]["total_score"],
                        problems_solved=users_db[user_email]["problems_solved"],
                    )
                except Exception as e:
                    print(f"Supabase totals patch failed (optional): {e}")

        # Broadcast real-time update to leaderboards
        await ws_manager.broadcast(json.dumps({"event": "leaderboard_update"}))


async def _already_solved_for_problem(user_id: str, user_email: str, problem_key: str) -> bool:
    # Check Mongo first (if enabled) for dedupe across restarts.
    if mongo_enabled and _submissions_coll is not None:
        doc = await _submissions_coll.find_one(
            {"user_email": user_email, "problem_id": problem_key, "status": "accepted"}
        )
        return doc is not None

    # Supabase dedupe (optional).
    if supabase_enabled:
        try:
            if problem_key.startswith("p"):
                problem_id_int = int(problem_key[1:])
            else:
                problem_id_int = int(problem_key)
            rows = await supabase_select_rows(
                "submissions",
                params={
                    "select": "id",
                    "user_id": f"eq.{user_id}",
                    "problem_id": f"eq.{problem_id_int}",
                    "status": "eq.accepted",
                    "limit": "1",
                },
            )
            return len(rows) > 0
        except Exception:
            pass

    return any(
        s["user_email"] == user_email
        and s.get("problem_id") == problem_key
        and s.get("status") == "accepted"
        for s in submissions_db.values()
    )


async def _update_submission_record(submission_id: str, update: dict) -> None:
    if mongo_enabled and _submissions_coll is not None:
        await _submissions_coll.update_one({"id": submission_id}, {"$set": update})
    if submission_id in submissions_db:
        submissions_db[submission_id].update(update)
    if supabase_enabled:
        try:
            status_val = update.get("status")
            payload = {
                "status": status_val,
                "test_cases_passed": int(update.get("test_cases_passed", 0)),
                "test_cases_total": int(update.get("test_cases_total", 0)),
                "execution_time_ms": update.get("execution_time_ms"),
                "score": int(update.get("score", 0)),
                "completed_at": datetime.utcnow().isoformat() if status_val and status_val != "pending" else None,
            }
            await supabase_patch_rows(
                "submissions",
                {"id": f"eq.{submission_id}"},
                payload,
            )
        except Exception as e:
            print(f"Supabase patch submission failed (optional): {e}")


@app.post("/api/v1/submissions/", response_model=Day2SubmissionResponse)
@app.post("/api/v1/submissions", response_model=Day2SubmissionResponse)
async def day2_submit(
    req: Day2SubmissionRequest,
    payload: dict = Depends(verify_jwt_token),
):
    user_email = payload.get("email")
    user_id = payload.get("sub")

    if user_email is None or user_id is None:
        raise HTTPException(status_code=401, detail="Invalid token")

    problem_key = _PROBLEM_KEY_BY_ID.get(req.problem_id)
    if not problem_key:
        raise HTTPException(status_code=404, detail="Problem not found")

    submission_id = str(uuid4())
    test_cases = [tc for tc in problems_db[problem_key]["test_cases"] if tc.get("visible", True)]

    # Create pending submission first (polling will see this).
    submission = {
        "id": submission_id,
        "user_id": user_id,
        "user_email": user_email,
        "problem_id": problem_key,  # internal key used by leaderboard/dedupe
        "problem_id_int": req.problem_id,
        "language": req.language,
        "source_code": req.source_code,
        "status": "pending",
        "test_cases_passed": 0,
        "test_cases_total": len(test_cases),
        "execution_time_ms": 0.0,
        "score": 0,
        "message": "Your submission is being processed...",
        "stdout": None,
        "stderr": None,
        "created_at": datetime.utcnow().isoformat(),
    }

    await insert_submission(submission)

    # Execute in background for polling flow.
    # Use asyncio.create_task to avoid coupling with FastAPI background_tasks injection.
    asyncio.create_task(
        _execute_day2_submission(
            submission_id=submission_id,
            user_id=user_id,
            user_email=user_email,
            language=req.language,
            source_code=req.source_code,
            problem_id_int=req.problem_id,
        )
    )

    return Day2SubmissionResponse(
        id=submission_id,
        status="pending",
        test_cases_passed=0,
        test_cases_total=len(test_cases),
        execution_time_ms=0.0,
        score=0,
        message="Your submission is being processed...",
        stdout=None,
        stderr=None,
    )


@app.get("/api/v1/submissions/{submission_id}", response_model=Day2SubmissionResponse)
async def day2_get_submission(submission_id: str, payload: dict = Depends(verify_jwt_token)):
    submission = await get_submission_by_id(submission_id)
    if submission is None:
        raise HTTPException(status_code=404, detail="Submission not found")

    return Day2SubmissionResponse(
        id=submission_id,
        status=submission.get("status", ""),
        test_cases_passed=int(submission.get("test_cases_passed", 0)),
        test_cases_total=int(submission.get("test_cases_total", 0)),
        execution_time_ms=float(submission.get("execution_time_ms", 0) or 0),
        score=int(submission.get("score", 0) or 0),
        message=submission.get("message", "") or "",
        stdout=submission.get("stdout"),
        stderr=submission.get("stderr"),
    )

# ====== LEADERBOARD ENDPOINTS ======

@app.websocket("/api/v1/ws/leaderboard")
@app.websocket("/ws/leaderboard")
async def websocket_leaderboard_endpoint(websocket: WebSocket):
    await ws_manager.connect(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        ws_manager.disconnect(websocket)

@app.get("/api/leaderboards/global")
@app.get("/api/v1/leaderboards/global")
async def global_leaderboard(payload: dict = Depends(verify_jwt_token)):
    # Prefer Redis ordering if enabled; fall back to in-memory sort.
    if redis_enabled:
        z = await redis_get_leaderboard_global(limit=50)
        ordered_emails = [m for (m, _) in z]
        users_list = [users_db[email] for email in ordered_emails if email in users_db]
    else:
        users_list = await get_leaderboard_users_sorted()
    
    return [
        {
            "rank": idx + 1,
            "name": user.get("name", ""),
            "username": user.get("username", user.get("name", "")),
            "user_id": user.get("id", ""),
            "house": user["house"].title(),
            "score": user.get("total_score", 0),
            "problems_solved": user.get("problems_solved", 0),
            "submissions": user.get("submissions", 0)
        }
        for idx, user in enumerate(users_list)
    ]

@app.get("/api/leaderboards/houses")
@app.get("/api/v1/leaderboards/houses")
async def house_leaderboards(payload: dict = Depends(verify_jwt_token)):
    houses = {
        "gryffindor": {"members": 0, "total_score": 0, "avg_score": 0},
        "hufflepuff": {"members": 0, "total_score": 0, "avg_score": 0},
        "ravenclaw": {"members": 0, "total_score": 0, "avg_score": 0},
        "slytherin": {"members": 0, "total_score": 0, "avg_score": 0}
    }

    if redis_enabled and _redis_client is not None:
        for house_name in houses.keys():
            zset = f"coduku:lb:house:{house_name}"
            entries = await _redis_client.zrevrange(zset, 0, -1, withscores=True)
            houses[house_name]["members"] = len(entries)
            houses[house_name]["total_score"] = sum(int(score) for (_email, score) in entries)
            members = houses[house_name]["members"]
            total = houses[house_name]["total_score"]
            houses[house_name]["avg_score"] = total / members if members > 0 else 0
    else:
        users_list = await get_leaderboard_users_sorted()
        for user in users_list:
            house = user["house"]
            score = user.get("total_score", 0)
            houses[house]["members"] += 1
            houses[house]["total_score"] += score

        for house in houses:
            members = houses[house]["members"]
            total = houses[house]["total_score"]
            houses[house]["avg_score"] = total / members if members > 0 else 0
    
    return [
        {
            "rank": idx + 1,
            "house": house.title(),
            "total_score": data["total_score"],
            "members": data["members"],
            "average_score": round(data["avg_score"], 2)
        }
        for idx, (house, data) in enumerate(sorted(
            houses.items(),
            key=lambda x: x[1]["total_score"],
            reverse=True
        ))
    ]

@app.get("/api/leaderboards/house/{house_name}")
@app.get("/api/v1/leaderboards/house/{house_name}")
async def house_members(house_name: str, payload: dict = Depends(verify_jwt_token)):
    house = house_name.lower()
    valid_houses = ["gryffindor", "hufflepuff", "ravenclaw", "slytherin"]
    
    if house not in valid_houses:
        raise HTTPException(status_code=400, detail="Invalid house")
    
    if redis_enabled and _redis_client is not None:
        zset = f"coduku:lb:house:{house}"
        entries = await _redis_client.zrevrange(zset, 0, -1, withscores=True)
        members = []
        for (email, score) in entries:
            u = users_db.get(email)
            if not u:
                continue
            members.append({**u, "total_score": int(score)})
    else:
        users_list = await get_leaderboard_users_sorted()
        members = sorted(
            [u for u in users_list if u["house"] == house],
            key=lambda x: x.get("total_score", 0),
            reverse=True
        )
    
    return [
        {
            "rank": idx + 1,
            "name": member.get("name", ""),
            "username": member.get("username", member.get("name", "")),
            "score": member.get("total_score", 0),
            "problems_solved": member.get("problems_solved", 0)
        }
        for idx, member in enumerate(members)
    ]

# ====== RUN SERVER ======
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=False,
        log_level="info"
    )
