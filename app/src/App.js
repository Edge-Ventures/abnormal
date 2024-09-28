import React, { useState } from 'react';
import axios from 'axios';

function App() {
  const [credentials, setCredentials] = useState({
    host: '',
    port: '',
    username: '',
    password: '',
    database: '',
    table: ''
  });
  const [results, setResults] = useState(null);

  const handleInputChange = (e) => {
    setCredentials({
      ...credentials,
      [e.target.name]: e.target.value
    });
  };

  const handleSubmit = async (e) => {
    e.preventDefault();
    try {
      const response = await axios.post('http://localhost:8000/analyze', credentials);
      setResults(response.data);
    } catch (error) {
      console.error('Error:', error);
    }
  };

  return (
    <div className="App">
      <h1>Exploratory Data Analysis</h1>
      <form onSubmit={handleSubmit}>
        <input name="host" placeholder="edgeblog2019-postgres.chkrgfabutfo.us-west-2.rds.amazonaws.com" onChange={handleInputChange} />
        <input name="port" placeholder="5432" onChange={handleInputChange} />
        <input name="username" placeholder="postgres" onChange={handleInputChange} />
        <input name="password" placeholder="donedge23" type="password" onChange={handleInputChange} />
        <input name="database" placeholder="real_estate_mogul" onChange={handleInputChange} />
        <input name="table" placeholder="debate" onChange={handleInputChange} />
        <button type="submit">Analyze</button>
      </form>

      {results && (
        <div>
          <h2>Analysis Results</h2>
          <h3>Pandas Profiling</h3>
          <pre>{JSON.stringify(results.pandas_profiling, null, 2)}</pre>
          <h3>Sweetviz</h3>
          <iframe src={results.sweetviz} width="100%" height="500px"></iframe>
          <h3>Autoviz</h3>
          <iframe src={results.autoviz} width="100%" height="500px"></iframe>
          <h3>D-Tale</h3>
          <iframe src={results.dtale} width="100%" height="500px"></iframe>
        </div>
      )}
    </div>
  );
}

export default App;