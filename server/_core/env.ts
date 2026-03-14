export const ENV = {
  appId: process.env.VITE_APP_ID ?? "",
  cookieSecret: process.env.JWT_SECRET || (process.env.NODE_ENV !== "production" ? "local_dev_secret_12345678" : ""),
  databaseUrl: process.env.DATABASE_URL ?? "",
  oAuthServerUrl: process.env.OAUTH_SERVER_URL ?? "",
  ownerOpenId: process.env.OWNER_OPEN_ID ?? "",
  isProduction: process.env.NODE_ENV === "production",
  forgeApiUrl: process.env.BUILT_IN_FORGE_API_URL ?? "",
  forgeApiKey: process.env.BUILT_IN_FORGE_API_KEY ?? "",
};

// Fail-fast em produção se o segredo do JWT estiver ausente
if (ENV.isProduction && !ENV.cookieSecret) {
  throw new Error("JWT_SECRET não definido em produção. Defina a variável de ambiente JWT_SECRET.");
}
