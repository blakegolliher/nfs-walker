export function EmptyState({ message = 'No data available' }: { message?: string }) {
  return (
    <div className="flex items-center justify-center py-8 text-sm text-gray-500">
      {message}
    </div>
  );
}
