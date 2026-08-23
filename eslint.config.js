import eslint from '@eslint/js';
import tseslint from 'typescript-eslint';

export default tseslint.config(
  { ignores: ['dist/**', 'node_modules/**', '.wrangler/**', 'coverage/**'] },
  eslint.configs.recommended,
  ...tseslint.configs.recommended,
  {
    languageOptions: { globals: { crypto: 'readonly', WebSocketPair: 'readonly' } },
    rules: { '@typescript-eslint/no-explicit-any': 'off' }
  }
);
