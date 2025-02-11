import React from 'react';
import { BrowserRouter as Router, Routes, Route } from 'react-router-dom';
import { ThemeProvider, createTheme } from '@mui/material/styles';
import CssBaseline from '@mui/material/CssBaseline';
import Dashboard from './components/Dashboard';
import Login from './components/Login';
import TokenizedData from './components/TokenizedData';
import Layout from './components/Layout';
import { AuthProvider } from './context/AuthContext';

// Create a dark theme with quantum-inspired colors
const theme = createTheme({
  palette: {
    mode: 'dark',
    primary: {
      main: '#6F2DFF', // Quantum purple
    },
    secondary: {
      main: '#00F0FF', // Quantum cyan
    },
    background: {
      default: '#0A0B1E', // Deep space black
      paper: '#141539', // Quantum field dark
    },
  },
  typography: {
    fontFamily: '"Roboto", "Helvetica", "Arial", sans-serif',
    h1: {
      fontSize: '2.5rem',
      fontWeight: 600,
    },
    h2: {
      fontSize: '2rem',
      fontWeight: 500,
    },
  },
  components: {
    MuiButton: {
      styleOverrides: {
        root: {
          borderRadius: 8,
          textTransform: 'none',
        },
      },
    },
    MuiCard: {
      styleOverrides: {
        root: {
          borderRadius: 16,
          backgroundImage: 'linear-gradient(45deg, rgba(111,45,255,0.05) 0%, rgba(0,240,255,0.05) 100%)',
        },
      },
    },
  },
});

function App() {
  return (
    <ThemeProvider theme={theme}>
      <CssBaseline />
      <AuthProvider>
        <Router>
          <Routes>
            <Route path="/login" element={<Login />} />
            <Route path="/" element={<Layout />}>
              <Route index element={<Dashboard />} />
              <Route path="data" element={<TokenizedData />} />
            </Route>
          </Routes>
        </Router>
      </AuthProvider>
    </ThemeProvider>
  );
}

export default App;
