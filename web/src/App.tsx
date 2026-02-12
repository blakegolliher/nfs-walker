import { Routes, Route } from 'react-router-dom';
import { Sidebar } from './components/layout/Sidebar';
import { TopBar } from './components/layout/TopBar';
import { OverviewPage } from './pages/OverviewPage';
import { CapacityPage } from './pages/CapacityPage';
import { FilesPage } from './pages/FilesPage';
import { OwnershipPage } from './pages/OwnershipPage';
import { DirectoriesPage } from './pages/DirectoriesPage';
import { QueryExplorerPage } from './pages/QueryExplorerPage';

export default function App() {
  return (
    <div className="flex h-screen overflow-hidden">
      <Sidebar />
      <div className="flex flex-1 flex-col overflow-hidden">
        <TopBar />
        <main className="flex-1 overflow-y-auto p-4">
          <Routes>
            <Route path="/" element={<OverviewPage />} />
            <Route path="/capacity" element={<CapacityPage />} />
            <Route path="/files" element={<FilesPage />} />
            <Route path="/ownership" element={<OwnershipPage />} />
            <Route path="/directories" element={<DirectoriesPage />} />
            <Route path="/queries" element={<QueryExplorerPage />} />
          </Routes>
        </main>
      </div>
    </div>
  );
}
