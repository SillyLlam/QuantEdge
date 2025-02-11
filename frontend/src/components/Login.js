import React, { useState } from 'react';
import { useNavigate } from 'react-router-dom';
import {
  Box,
  Button,
  Card,
  CardContent,
  TextField,
  Typography,
  Container,
  Alert,
} from '@mui/material';
import { LoadingButton } from '@mui/lab';
import { useAuth } from '../context/AuthContext';

const Login = () => {
  const navigate = useNavigate();
  const { login } = useAuth();
  const [username, setUsername] = useState('');
  const [password, setPassword] = useState('');
  const [error, setError] = useState('');
  const [loading, setLoading] = useState(false);

  const handleSubmit = async (e) => {
    e.preventDefault();
    setError('');
    setLoading(true);

    try {
      await login(username, password);
      navigate('/');
    } catch (err) {
      setError('Failed to login. Please check your credentials.');
    } finally {
      setLoading(false);
    }
  };

  return (
    <Box
      sx={{
        minHeight: '100vh',
        display: 'flex',
        alignItems: 'center',
        background: 'linear-gradient(45deg, #0A0B1E 30%, #141539 90%)',
      }}
    >
      <Container maxWidth="sm">
        <Card
          sx={{
            backdropFilter: 'blur(10px)',
            backgroundColor: 'rgba(20, 21, 57, 0.8)',
            borderRadius: 4,
            boxShadow: '0 8px 32px 0 rgba(31, 38, 135, 0.37)',
            border: '1px solid rgba(255, 255, 255, 0.18)',
          }}
        >
          <CardContent sx={{ p: 4 }}>
            <Box
              component="form"
              onSubmit={handleSubmit}
              sx={{
                display: 'flex',
                flexDirection: 'column',
                gap: 3,
              }}
            >
              <Box sx={{ textAlign: 'center', mb: 3 }}>
                <Typography
                  variant="h4"
                  component="h1"
                  sx={{
                    fontWeight: 700,
                    background: 'linear-gradient(45deg, #6F2DFF 30%, #00F0FF 90%)',
                    backgroundClip: 'text',
                    textFillColor: 'transparent',
                    WebkitBackgroundClip: 'text',
                    WebkitTextFillColor: 'transparent',
                  }}
                >
                  Quantum-AI Tokenization
                </Typography>
                <Typography
                  variant="subtitle1"
                  sx={{ color: 'text.secondary', mt: 1 }}
                >
                  Secure access to your tokenized data
                </Typography>
              </Box>

              {error && (
                <Alert severity="error" sx={{ mb: 2 }}>
                  {error}
                </Alert>
              )}

              <TextField
                label="Username"
                variant="outlined"
                fullWidth
                value={username}
                onChange={(e) => setUsername(e.target.value)}
                required
                sx={{
                  '& .MuiOutlinedInput-root': {
                    '& fieldset': {
                      borderColor: 'rgba(111, 45, 255, 0.3)',
                    },
                    '&:hover fieldset': {
                      borderColor: 'rgba(111, 45, 255, 0.5)',
                    },
                    '&.Mui-focused fieldset': {
                      borderColor: '#6F2DFF',
                    },
                  },
                }}
              />

              <TextField
                label="Password"
                type="password"
                variant="outlined"
                fullWidth
                value={password}
                onChange={(e) => setPassword(e.target.value)}
                required
                sx={{
                  '& .MuiOutlinedInput-root': {
                    '& fieldset': {
                      borderColor: 'rgba(111, 45, 255, 0.3)',
                    },
                    '&:hover fieldset': {
                      borderColor: 'rgba(111, 45, 255, 0.5)',
                    },
                    '&.Mui-focused fieldset': {
                      borderColor: '#6F2DFF',
                    },
                  },
                }}
              />

              <LoadingButton
                type="submit"
                variant="contained"
                fullWidth
                loading={loading}
                sx={{
                  mt: 2,
                  py: 1.5,
                  background: 'linear-gradient(45deg, #6F2DFF 30%, #00F0FF 90%)',
                  '&:hover': {
                    background: 'linear-gradient(45deg, #5f25d9 30%, #00d8e6 90%)',
                  },
                }}
              >
                Login
              </LoadingButton>
            </Box>
          </CardContent>
        </Card>
      </Container>
    </Box>
  );
};

export default Login;
