import { ReactNode, useEffect, useState, useRef } from 'react';
import { useRouter } from 'next/router';
import Link from 'next/link';
import { useAuth } from '@/contexts/AuthContext';

interface LayoutProps {
  children: ReactNode;
}

export default function Layout({ children }: LayoutProps) {
  const { user, loading, logout } = useAuth();
  const router = useRouter();
  const [adminDropdownOpen, setAdminDropdownOpen] = useState(false);
  const dropdownRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (!loading && !user) {
      router.push('/login');
    }
  }, [user, loading, router]);

  // Close dropdown when clicking outside
  useEffect(() => {
    const handleClickOutside = (event: MouseEvent) => {
      if (dropdownRef.current && !dropdownRef.current.contains(event.target as Node)) {
        setAdminDropdownOpen(false);
      }
    };
    document.addEventListener('mousedown', handleClickOutside);
    return () => document.removeEventListener('mousedown', handleClickOutside);
  }, []);

  if (loading || !user) {
    return <div className="min-h-screen flex items-center justify-center">Loading...</div>;
  }

  const isAdminPage = ['/transactions', '/groups', '/upload'].includes(router.pathname);

  return (
    <div className="min-h-screen bg-gray-100">
      {/* Navigation */}
      <nav className="bg-white shadow-sm">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
          <div className="flex justify-between h-16">
            <div className="flex">
              <div className="flex-shrink-0 flex items-center">
                <h1 className="text-xl font-bold">Portfolio Monitor</h1>
              </div>
              <div className="ml-6 flex space-x-8">
                <Link href="/" className="inline-flex items-center px-1 pt-1 text-sm font-medium border-b-2 border-transparent hover:border-blue-500">
                  Dashboard
                </Link>
                <Link href="/statistics" className="inline-flex items-center px-1 pt-1 text-sm font-medium border-b-2 border-transparent hover:border-blue-500">
                  Statistics
                </Link>
                <Link href="/new-funds" className="inline-flex items-center px-1 pt-1 text-sm font-medium border-b-2 border-transparent hover:border-blue-500">
                  New Funds
                </Link>
                <Link href="/coverage" className="inline-flex items-center px-1 pt-1 text-sm font-medium border-b-2 border-transparent hover:border-blue-500">
                  Active Coverage
                </Link>
                <Link href="/ideas" className="inline-flex items-center px-1 pt-1 text-sm font-medium border-b-2 border-transparent hover:border-blue-500">
                  Idea Pipeline
                </Link>
                <Link href="/tax" className="inline-flex items-center px-1 pt-1 text-sm font-medium border-b-2 border-transparent hover:border-blue-500">
                  Tax Optimization
                </Link>
                {/* Admin Dropdown */}
                <div className="relative" ref={dropdownRef}>
                  <button
                    onClick={() => setAdminDropdownOpen(!adminDropdownOpen)}
                    className={`inline-flex items-center px-1 pt-1 text-sm font-medium border-b-2 ${
                      isAdminPage ? 'border-blue-500 text-blue-600' : 'border-transparent hover:border-blue-500'
                    }`}
                  >
                    Admin
                    <svg
                      className={`ml-1 h-4 w-4 transition-transform ${adminDropdownOpen ? 'rotate-180' : ''}`}
                      fill="none"
                      stroke="currentColor"
                      viewBox="0 0 24 24"
                    >
                      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
                    </svg>
                  </button>
                  {adminDropdownOpen && (
                    <div className="absolute left-0 mt-2 w-48 bg-white rounded-md shadow-lg py-1 z-50 border border-gray-200">
                      <Link
                        href="/transactions"
                        className={`block px-4 py-2 text-sm hover:bg-gray-100 ${
                          router.pathname === '/transactions' ? 'bg-blue-50 text-blue-600' : 'text-gray-700'
                        }`}
                        onClick={() => setAdminDropdownOpen(false)}
                      >
                        Transactions
                      </Link>
                      <Link
                        href="/groups"
                        className={`block px-4 py-2 text-sm hover:bg-gray-100 ${
                          router.pathname === '/groups' ? 'bg-blue-50 text-blue-600' : 'text-gray-700'
                        }`}
                        onClick={() => setAdminDropdownOpen(false)}
                      >
                        Groups
                      </Link>
                      <Link
                        href="/upload"
                        className={`block px-4 py-2 text-sm hover:bg-gray-100 ${
                          router.pathname === '/upload' ? 'bg-blue-50 text-blue-600' : 'text-gray-700'
                        }`}
                        onClick={() => setAdminDropdownOpen(false)}
                      >
                        Data Import
                      </Link>
                    </div>
                  )}
                </div>
              </div>
            </div>
            <div className="flex items-center">
              <span className="mr-4 text-sm text-gray-700">{user.email}</span>
              <button onClick={logout} className="btn btn-secondary text-sm">
                Logout
              </button>
            </div>
          </div>
        </div>
      </nav>

      {/* Main content */}
      <main className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        {children}
      </main>
    </div>
  );
}
