import React, { useState, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import { useAuthStore } from '../store/authStore';
import { authAPI } from '../services/apiService';
import '../styles/AuthPage.css';

const houses = [
  { value: 'gryffindor', label: 'Gryffindor', emoji: '🦁', desc: 'Brave & Bold',       color: '#c8102e', gradient: 'linear-gradient(135deg, #b91c1c, #f59e0b)' },
  { value: 'hufflepuff', label: 'Hufflepuff', emoji: '🦡', desc: 'Loyal & Kind',        color: '#e4a800', gradient: 'linear-gradient(135deg, #ca8a04, #422006)' },
  { value: 'ravenclaw',  label: 'Ravenclaw',  emoji: '🦅', desc: 'Wise & Creative',     color: '#0e4d92', gradient: 'linear-gradient(135deg, #1d4ed8, #92400e)' },
  { value: 'slytherin',  label: 'Slytherin',  emoji: '🐍', desc: 'Cunning & Ambitious', color: '#1a7a3a', gradient: 'linear-gradient(135deg, #15803d, #475569)' },
];

/* Sorting hat messages shown while "thinking" */
const hatThoughts = [
  'Hmm… I sense great potential…',
  'Let me look deeper into your mind…',
  'Courage? Loyalty? Wisdom? Ambition?',
  'The choice is becoming clear…',
  'Yes… I know exactly where you belong!',
];

function SortingHatCeremony({ username, onDone }) {
  const [phase, setPhase] = useState('thinking'); // thinking | reveal
  const [thoughtIdx, setThoughtIdx] = useState(0);
  const [assignedHouse] = useState(() => houses[Math.floor(Math.random() * houses.length)]);

  /* Cycle through hat thoughts */
  useEffect(() => {
    if (phase !== 'thinking') return;
    const interval = setInterval(() => {
      setThoughtIdx(i => {
        if (i >= hatThoughts.length - 1) {
          clearInterval(interval);
          setTimeout(() => setPhase('reveal'), 600);
          return i;
        }
        return i + 1;
      });
    }, 900);
    return () => clearInterval(interval);
  }, [phase]);

  /* After reveal, call parent with the chosen house after a short pause */
  useEffect(() => {
    if (phase === 'reveal') {
      const t = setTimeout(() => onDone(assignedHouse), 2800);
      return () => clearTimeout(t);
    }
  }, [phase, assignedHouse, onDone]);

  return (
    <div className="sorting-ceremony">
      {/* Avatar with hat on top */}
      <div className="sorting-avatar-wrap">
        <div className="sorting-hat-emoji">🎩</div>
        <div className="sorting-avatar-emoji">🧙</div>
      </div>

      <p className="sorting-username">{username}</p>

      {phase === 'thinking' ? (
        <div className="sorting-thought animate-fade-in" key={thoughtIdx}>
          <span className="sorting-dots">
            <span /><span /><span />
          </span>
          <p>{hatThoughts[thoughtIdx]}</p>
        </div>
      ) : (
        <div className="sorting-reveal animate-fade-in">
          <div
            className="sorting-house-badge"
            style={{ background: assignedHouse.gradient }}
          >
            <span className="sorting-house-emoji">{assignedHouse.emoji}</span>
            <span className="sorting-house-name">{assignedHouse.label}!</span>
          </div>
          <p className="sorting-house-desc">{assignedHouse.desc}</p>
          <p className="sorting-entering">Entering the arena…</p>
        </div>
      )}
    </div>
  );
}

function AuthPage() {
  const [isLogin, setIsLogin] = useState(true);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const [sorting, setSorting] = useState(false);   // show sorting hat screen
  const [pendingData, setPendingData] = useState(null); // {response, username}
  const [form, setForm] = useState({ username: '', email: '', password: '' });

  const navigate = useNavigate();
  const { login } = useAuthStore();

  const handleChange = (e) => {
    setForm({ ...form, [e.target.name]: e.target.value });
    if (error) setError('');
  };

  const handleSubmit = async (e) => {
    e.preventDefault();
    setLoading(true);
    setError('');

    try {
      if (isLogin) {
        /* LOGIN — house comes from backend */
        const response = await authAPI.login({ email: form.email, password: form.password });
        const token = response.access_token;
        const displayName = response.username || response.name || form.email.split('@')[0];
        const userHouse = response.house || 'gryffindor';

        login(token, {
          user_id: response.user_id,
          username: displayName,
          email: form.email || response.email,
          house: userHouse,
          total_score: response.total_score || 0,
          problems_solved: response.problems_solved || 0,
        });
        navigate('/dashboard');
      } else {
        /* REGISTER — show sorting hat first, then register with chosen house */
        const displayName = form.username || form.email.split('@')[0];
        setPendingData({ form, displayName });
        setSorting(true);
      }
    } catch (err) {
      const errorMsg = err.detail || err.message || 'Authentication failed. Please try again.';
      setError(errorMsg);
    } finally {
      setLoading(false);
    }
  };

  /* Called by SortingHatCeremony once it has picked a house */
  const handleHouseDecided = async (house) => {
    try {
      const { form: savedForm, displayName } = pendingData;
      const response = await authAPI.register({
        username: displayName,
        email: savedForm.email,
        password: savedForm.password,
        house: house.value,
      });

      const token = response.access_token;
      const userHouse = response.house || house.value;

      login(token, {
        user_id: response.user_id,
        username: response.username || response.name || displayName,
        email: savedForm.email || response.email,
        house: userHouse,
        total_score: response.total_score || 0,
        problems_solved: response.problems_solved || 0,
      });

      navigate('/dashboard');
    } catch (err) {
      setSorting(false);
      const errorMsg = err.detail || err.message || 'Registration failed. Please try again.';
      setError(errorMsg);
    }
  };

  /* ── Sorting hat ceremony screen ── */
  if (sorting && pendingData) {
    return (
      <div className="auth-page">
        <div className="auth-bg-effects">
          <div className="bg-orb bg-orb-1" />
          <div className="bg-orb bg-orb-2" />
          <div className="bg-orb bg-orb-3" />
        </div>
        <div className="sorting-container">
          <SortingHatCeremony
            username={pendingData.displayName}
            onDone={handleHouseDecided}
          />
        </div>
      </div>
    );
  }

  /* ── Normal auth form ── */
  return (
    <div className="auth-page">
      <div className="auth-bg-effects">
        <div className="bg-orb bg-orb-1" />
        <div className="bg-orb bg-orb-2" />
        <div className="bg-orb bg-orb-3" />
      </div>

      <div className="auth-container">
        {/* Left branding panel */}
        <div className="auth-branding">
          <div className="brand-content">
            <div className="brand-logo-area">
              <span className="brand-icon">⚡</span>
              <h1 className="brand-title">CODUKU</h1>
            </div>
            <p className="brand-tagline">Where Code Meets Competition</p>
            <div className="brand-features">
              <div className="brand-feature">
                <span className="feature-icon">🏰</span>
                <div>
                  <strong>House System</strong>
                  <p>The Sorting Hat decides your destiny</p>
                </div>
              </div>
              <div className="brand-feature">
                <span className="feature-icon">⚔️</span>
                <div>
                  <strong>Code Arena</strong>
                  <p>Solve challenges with a powerful editor</p>
                </div>
              </div>
              <div className="brand-feature">
                <span className="feature-icon">🏆</span>
                <div>
                  <strong>Live Leaderboards</strong>
                  <p>Rise through the ranks in real-time</p>
                </div>
              </div>
              <div className="brand-feature">
                <span className="feature-icon">🧙</span>
                <div>
                  <strong>AI Mentor</strong>
                  <p>Get magical hints when you're stuck</p>
                </div>
              </div>
            </div>
          </div>
        </div>

        {/* Right form panel */}
        <div className="auth-form-panel">
          <div className="auth-form-header">
            <h2>{isLogin ? 'Welcome Back, Wizard' : 'Join the Arena'}</h2>
            <p>{isLogin ? 'Sign in to continue your quest' : 'Create your account — the Hat decides your house!'}</p>
          </div>

          <div className="auth-toggle">
            <button
              type="button"
              className={`toggle-btn ${isLogin ? 'active' : ''}`}
              onClick={() => { setIsLogin(true); setError(''); }}
            >
              Sign In
            </button>
            <button
              type="button"
              className={`toggle-btn ${!isLogin ? 'active' : ''}`}
              onClick={() => { setIsLogin(false); setError(''); }}
            >
              Register
            </button>
          </div>

          <form onSubmit={handleSubmit} className="auth-form">
            {!isLogin && (
              <div className="form-group animate-fade-in-up">
                <label htmlFor="auth-username">Wizard Name</label>
                <input
                  id="auth-username"
                  type="text"
                  name="username"
                  placeholder="Choose your wizard name"
                  value={form.username}
                  onChange={handleChange}
                  required
                  autoComplete="username"
                />
              </div>
            )}

            <div className="form-group">
              <label htmlFor="auth-email">Email</label>
              <input
                id="auth-email"
                type="email"
                name="email"
                placeholder="you@college.edu"
                value={form.email}
                onChange={handleChange}
                required
                autoComplete="email"
              />
            </div>

            <div className="form-group">
              <label htmlFor="auth-password">Password</label>
              <input
                id="auth-password"
                type="password"
                name="password"
                placeholder="••••••••"
                value={form.password}
                onChange={handleChange}
                required
                minLength="6"
                autoComplete={isLogin ? 'current-password' : 'new-password'}
              />
            </div>

            {/* Sorting hat teaser shown during register */}
            {!isLogin && (
              <div className="sorting-teaser animate-fade-in-up">
                <span className="teaser-hat">🎩</span>
                <p>The Sorting Hat will decide your house after you register!</p>
              </div>
            )}

            {error && (
              <div className="auth-error animate-fade-in">
                <span>⚠️</span> {error}
              </div>
            )}

            <button type="submit" disabled={loading} className="auth-submit-btn" id="auth-submit">
              {loading ? (
                <span className="loading-spinner">
                  <span className="spinner" />
                  Casting Spell…
                </span>
              ) : (
                isLogin ? '✨ Sign In' : '✨ Create Account'
              )}
            </button>
          </form>
        </div>
      </div>
    </div>
  );
}

export default AuthPage;
