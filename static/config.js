window.POKEMON_TCG_CONFIG = Object.assign(
  {
    // Set this to your deployed backend URL, e.g. "https://api.example.com".
    apiBaseUrl: '',
    // Reserved for future static-data mode exports.
    dataBaseUrl: './data',
    mode: 'api',
    // Require explicit API URL on Pages/project hosts by default.
    requireExplicitApiBaseUrl: true,
  },
  window.POKEMON_TCG_CONFIG || {},
);

window.__POKEMON_TCG_CONFIG_LOADED__ = true;
