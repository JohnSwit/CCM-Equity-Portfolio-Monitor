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
  // Initialize from cache so the page renders immediately without a loading spinner
  const cachedUser = getCachedUser();
  const hasToken = typeof window !== 'undefined' && !!localStorage.getItem('token');
  const [user, setUser] = useState<User | null>(cachedUser);
  const [loading, setLoading] = useState(hasToken && !cachedUser);
  const router = useRouter();

  useEffect(() => {
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
    } catch (error) {
      localStorage.removeItem('token');
      setCachedUser(null);
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
