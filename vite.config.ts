import { defineConfig } from "vite-plus";

export default defineConfig({
  test: {
    environment: "node",
    include: ["src/**/*.test.ts"],
  },
  lint: {
    ignorePatterns: ["dist/**", ".fallow/**", "node_modules/**"],
    options: {
      typeAware: true,
      typeCheck: true,
    },
  },
  fmt: {
    ignorePatterns: [".agents/**", "dist/**", ".fallow/**", "node_modules/**"],
  },
  pack: {
    entry: ["src/main.ts"],
    dts: false,
    format: ["esm"],
    sourcemap: true,
  },
});
