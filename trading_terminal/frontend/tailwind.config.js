/** @type {import('tailwindcss').Config} */
module.exports = {
  content: [
    "./pages/**/*.{js,ts,jsx,tsx,mdx}",
    "./components/**/*.{js,ts,jsx,tsx,mdx}",
    "./app/**/*.{js,ts,jsx,tsx,mdx}",
  ],
  theme: {
    extend: {
      colors: {
        terminal: {
          bg: "#0d0f14",
          panel: "#13161d",
          border: "#1e2433",
          green: "#00c853",
          red: "#ff1744",
          yellow: "#ffd600",
          blue: "#2979ff",
          muted: "#4a5568",
          text: "#e2e8f0",
        },
      },
      fontFamily: {
        mono: ["JetBrains Mono", "Fira Code", "monospace"],
      },
    },
  },
  plugins: [],
};
