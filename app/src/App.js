// App.js
import React, { useState, useEffect } from 'react';
import axios from 'axios';

function App() {
  const [analysisResults, setAnalysisResults] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  useEffect(() => {
    const fetchData = async () => {
      setLoading(true);
      try {
        const response = await axios.get('http://localhost:8888/analyze');
        setAnalysisResults(JSON.parse(response.data.results));
      } catch (err) {
        setError('Error fetching analysis results');
        console.error(err);
      }
      setLoading(false);
    };

    fetchData();
  }, []);

  if (loading) return <div>Loading...</div>;
  if (error) return <div>{error}</div>;
  if (!analysisResults) return <div>No data available</div>;

  return (
    <div className="App">
      <h1>Exploratory Data Analysis Results</h1>
      {Object.entries(analysisResults).map(([key, value]) => (
        <div key={key}>
          <h2>{key}</h2>
          {typeof value === 'string' && value.startsWith('<svg') ? (
            <div dangerouslySetInnerHTML={{ __html: value }} />
          ) : (
            <pre>{JSON.stringify(value, null, 2)}</pre>
          )}
        </div>
      ))}
    </div>
  );
}

export default App;