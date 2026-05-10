window.POKEMON_TCG_CONFIG = Object.assign(
  {
    // Set this to your deployed backend URL, e.g. "https://api.example.com".
    apiBaseUrl: '',
    // Reserved for future static-data mode exports.
    dataBaseUrl: './data',
    mode: 'api',
  },
  window.POKEMON_TCG_CONFIG || {},
);
