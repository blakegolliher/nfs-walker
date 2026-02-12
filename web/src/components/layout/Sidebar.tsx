import { NavLink } from 'react-router-dom';
import { NAV_ITEMS } from '../../lib/constants';

const ICONS: Record<string, string> = {
  'chart-bar': 'M 3 17 V 9 h 4 v 8 H 3 Z M 9 17 V 5 h 4 v 12 H 9 Z M 15 17 v -6 h 4 v 6 h -4 Z',
  database: 'M 4 6 c 0 -1.7 3.6 -3 8 -3 s 8 1.3 8 3 v 12 c 0 1.7 -3.6 3 -8 3 s -8 -1.3 -8 -3 V 6 Z M 4 10 c 0 1.7 3.6 3 8 3 s 8 -1.3 8 -3 M 4 14 c 0 1.7 3.6 3 8 3 s 8 -1.3 8 -3',
  document: 'M 7 3 h 6 l 4 4 v 10 a 2 2 0 0 1 -2 2 H 9 a 2 2 0 0 1 -2 -2 V 3 Z M 13 3 v 4 h 4',
  users: 'M 12 11 a 4 4 0 1 0 0 -8 a 4 4 0 0 0 0 8 Z m -7 10 a 7 7 0 0 1 14 0',
  folder: 'M 3 6 a 2 2 0 0 1 2 -2 h 4 l 2 2 h 6 a 2 2 0 0 1 2 2 v 8 a 2 2 0 0 1 -2 2 H 5 a 2 2 0 0 1 -2 -2 V 6 Z',
  terminal: 'M 4 17 l 6 -5 l -6 -5 M 12 17 h 8',
};

export function Sidebar() {
  return (
    <aside className="flex w-48 flex-col border-r border-border bg-surface-1">
      <div className="flex h-12 items-center gap-2 border-b border-border px-4">
        <div className="h-5 w-5 rounded bg-accent/80" />
        <span className="text-sm font-semibold text-gray-200">NFS Walker</span>
      </div>
      <nav className="flex-1 space-y-0.5 p-2">
        {NAV_ITEMS.map((item) => (
          <NavLink
            key={item.path}
            to={item.path}
            end={item.path === '/'}
            className={({ isActive }) =>
              `flex items-center gap-2.5 rounded px-3 py-1.5 text-sm transition-colors ${
                isActive
                  ? 'bg-accent/15 text-accent-hover font-medium'
                  : 'text-gray-400 hover:bg-surface-3 hover:text-gray-200'
              }`
            }
          >
            <svg className="h-4 w-4 shrink-0" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth={1.5} strokeLinecap="round" strokeLinejoin="round">
              <path d={ICONS[item.icon]} />
            </svg>
            {item.label}
          </NavLink>
        ))}
      </nav>
    </aside>
  );
}
