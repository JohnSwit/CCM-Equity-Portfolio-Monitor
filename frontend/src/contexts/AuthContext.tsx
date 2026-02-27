import React, { createContext, useContext, useState, useEffect, ReactNode } from 'react';
import { useRouter } from 'next/router';
import { api } from '@/lib/api';

interface User {
  id: number;
  email: string;
  full_name: string;
  is_admin: boolean;
}

interface AuthContextType {
  user: User | null;
  loading: boolean;
  login: (email: string, password: string) => Promise<void>;
  logout: () => void;
}

const AuthContext = createContext<AuthContextType | undefined>(undefined);

// Try to restore cached user from sessionStorage for instant render
function getCachedUser(): User | null {
  try {
    const cached = sessionStorage.getItem('cached_user');
    if (cached) return JSON.parse(cached);
  } catch {}
  return null;
}

function setCachedUser(user: User | null) {
  try {
    if (user) sessionStorage.setItem('cached_user', JSON.stringify(user));
    else sessionStorage.removeItem('cached_user');
  } catch {}
}

export function AuthProvider({ children }: { children: ReactNode }) {
  // Always start with null/true to match server render and avoid hydration mismatch.
  // Cache restore happens in useEffect (client-only).
  const [user, setUser] = useState<User | null>(null);
  const [loading, setLoading] = useState(true);
  const router = useRouter();

  useEffect(() => {
    // Instantly restore cached user so the UI doesn't flash a spinner
    const cached = getCachedUser();
    if (cached) {
      setUser(cached);
      setLoading(false);
    }
    // Then verify token with the API
    checkAuth();
  }, []);

  const checkAuth = async () => {
    try {
      const token = localStorage.getItem('token');
      if (token) {
        const userData = await api.getMe();
        setUser(userData);
        setCachedUser(userData);
      }
    } catch (error: any) {
      // Only log out on explicit auth failures (401/403).
      // Transient errors (network timeout, 500 from DB load during worker update)
      // should NOT clear the token — the user is still authenticated.
      const status = error?.response?.status;
      if (status === 401 || status === 403) {
        localStorage.removeItem('token');
        setCachedUser(null);
        setUser(null);
      } else {
        // Keep the cached user on transient failures so the app stays usable
        const cached = getCachedUser();
        if (cached) {
          setUser(cached);
        }
      }
    } finally {
      setLoading(false);
    }
  };

  const login = async (email: string, password: string) => {
    const data = await api.login(email, password);
    localStorage.setItem('token', data.access_token);
    const userData = await api.getMe();
    setUser(userData);
    router.push('/');
  };

  const logout = () => {
    localStorage.removeItem('token');
    setCachedUser(null);
    setUser(null);
    router.push('/login');
  };

  return (
    <AuthContext.Provider value={{ user, loading, login, logout }}>
      {children}
    </AuthContext.Provider>
  );
}

export function useAuth() {
  const context = useContext(AuthContext);
  if (context === undefined) {
    throw new Error('useAuth must be used within an AuthProvider');
  }
  return context;
}
