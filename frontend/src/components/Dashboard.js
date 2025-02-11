import React, { useState, useEffect } from 'react';
import {
  Box,
  Grid,
  Card,
  CardContent,
  Typography,
  CircularProgress,
} from '@mui/material';
import {
  AreaChart,
  Area,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
} from 'recharts';
import { useAuth } from '../context/AuthContext';
import api from '../services/api';

const StatCard = ({ title, value, loading }) => (
  <Card sx={{ height: '100%' }}>
    <CardContent>
      <Typography color="textSecondary" gutterBottom>
        {title}
      </Typography>
      {loading ? (
        <CircularProgress size={24} />
      ) : (
        <Typography variant="h4" component="div">
          {value}
        </Typography>
      )}
    </CardContent>
  </Card>
);

const Dashboard = () => {
  const { token } = useAuth();
  const [stats, setStats] = useState({
    totalTokens: 0,
    activeTokens: 0,
    tokenOperations24h: 0,
  });
  const [loading, setLoading] = useState(true);
  const [chartData, setChartData] = useState([]);

  useEffect(() => {
    const fetchStats = async () => {
      try {
        const response = await api.get('/stats', {
          headers: { Authorization: `Bearer ${token}` },
        });
        setStats(response.data);
        
        // Simulate chart data (replace with real data in production)
        const data = Array.from({ length: 24 }, (_, i) => ({
          time: `${i}:00`,
          tokens: Math.floor(Math.random() * 100),
        }));
        setChartData(data);
        
        setLoading(false);
      } catch (error) {
        console.error('Error fetching stats:', error);
        setLoading(false);
      }
    };

    fetchStats();
  }, [token]);

  return (
    <Box sx={{ flexGrow: 1, p: 3 }}>
      <Typography variant="h4" gutterBottom sx={{ mb: 4 }}>
        Quantum-AI Token Dashboard
      </Typography>

      <Grid container spacing={3} sx={{ mb: 4 }}>
        <Grid item xs={12} md={4}>
          <StatCard
            title="Total Tokens Generated"
            value={stats.totalTokens}
            loading={loading}
          />
        </Grid>
        <Grid item xs={12} md={4}>
          <StatCard
            title="Active Tokens"
            value={stats.activeTokens}
            loading={loading}
          />
        </Grid>
        <Grid item xs={12} md={4}>
          <StatCard
            title="Token Operations (24h)"
            value={stats.tokenOperations24h}
            loading={loading}
          />
        </Grid>
      </Grid>

      <Card sx={{ p: 2 }}>
        <Typography variant="h6" gutterBottom>
          Token Generation Activity (24h)
        </Typography>
        <Box sx={{ height: 300 }}>
          <ResponsiveContainer width="100%" height="100%">
            <AreaChart data={chartData}>
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis dataKey="time" />
              <YAxis />
              <Tooltip />
              <Area
                type="monotone"
                dataKey="tokens"
                stroke="#6F2DFF"
                fill="url(#colorTokens)"
              />
              <defs>
                <linearGradient id="colorTokens" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="5%" stopColor="#6F2DFF" stopOpacity={0.8} />
                  <stop offset="95%" stopColor="#6F2DFF" stopOpacity={0} />
                </linearGradient>
              </defs>
            </AreaChart>
          </ResponsiveContainer>
        </Box>
      </Card>
    </Box>
  );
};

export default Dashboard;
