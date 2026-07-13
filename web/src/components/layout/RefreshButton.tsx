import { useMutation, useQueryClient } from '@tanstack/react-query';
import { api } from '../../api/client';

// Rescans the server's data directory, then invalidates all cached
// query results so every page refetches against the new scan set.
export function RefreshButton() {
  const queryClient = useQueryClient();
  const { mutate, isPending } = useMutation({
    mutationFn: api.reloadScans,
    onSuccess: () => queryClient.invalidateQueries(),
  });

  return (
    <button
      onClick={() => mutate()}
      disabled={isPending}
      title="Rescan data directory"
      className="rounded border border-border bg-surface-2 p-1.5 text-gray-400 hover:border-accent hover:text-gray-200 disabled:opacity-50"
    >
      <svg
        viewBox="0 0 24 24"
        fill="none"
        stroke="currentColor"
        strokeWidth="2"
        strokeLinecap="round"
        strokeLinejoin="round"
        className={`h-3.5 w-3.5 ${isPending ? 'animate-spin' : ''}`}
      >
        <polyline points="23 4 23 10 17 10" />
        <path d="M20.49 15a9 9 0 1 1-2.12-9.36L23 10" />
      </svg>
    </button>
  );
}
