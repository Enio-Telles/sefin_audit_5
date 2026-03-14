import { COOKIE_NAME, ONE_YEAR_MS } from "@shared/const";
import type { Express, Request, Response } from "express";
import * as db from "../db";
import { getSessionCookieOptions } from "./cookies";
import { sdk } from "./sdk";

function getQueryParam(req: Request, key: string): string | undefined {
  const value = req.query[key];
  return typeof value === "string" ? value : undefined;
}

export function registerOAuthRoutes(app: Express) {
  // --- MOCK OAUTH FOR DEVELOPMENT ---
  if (process.env.NODE_ENV !== "production") {
    console.log("[Mock OAuth] Registering mock routes...");

    // Mock login page (app-auth)
    app.get("/mock-oauth/app-auth", (req: Request, res: Response) => {
      console.log("[Mock OAuth] GET /mock-oauth/app-auth reached");
      const redirectUri = req.query.redirectUri as string;
      const state = req.query.state as string;
      
      if (!redirectUri || !state) {
        return res.status(400).send("redirectUri and state are required");
      }

      // Simulate the redirect back to the app callback
      const callbackUrl = new URL(redirectUri);
      callbackUrl.searchParams.set("code", "mock_code_123");
      callbackUrl.searchParams.set("state", state);
      
      console.log("[Mock OAuth] Redirecting to", callbackUrl.toString());
      res.redirect(callbackUrl.toString());
    });

    // Handle both with and without /mock-oauth prefix to be safe
    const mockPostPaths = [
      "/webdev.v1.WebDevAuthPublicService/ExchangeToken",
      "/mock-oauth/webdev.v1.WebDevAuthPublicService/ExchangeToken",
      "/webdev.v1.WebDevAuthPublicService/GetUserInfo",
      "/mock-oauth/webdev.v1.WebDevAuthPublicService/GetUserInfo",
      "/webdev.v1.WebDevAuthPublicService/GetUserInfoWithJwt",
      "/mock-oauth/webdev.v1.WebDevAuthPublicService/GetUserInfoWithJwt"
    ];

    app.post(mockPostPaths, (req: Request, res: Response) => {
      console.log("[Mock OAuth] POST", req.path, "reached");
      
      if (req.path.includes("ExchangeToken")) {
        return res.json({
          accessToken: "mock_access_token_123",
          tokenType: "Bearer",
          expiresIn: 3600,
          scope: "all",
          idToken: "mock_id_token_123"
        });
      }

      if (req.path.includes("GetUserInfo")) {
        return res.json({
          openId: "dev-usuario-padrao",
          name: "Desenvolvedor Local",
          email: "dev@sefin.ro.gov.br",
          platform: "email",
          projectId: "sefin-audit-tool"
        });
      }

      res.status(404).json({ error: "Mock path not handled" });
    });
  }

  app.get("/api/oauth/callback", async (req: Request, res: Response) => {
    const code = getQueryParam(req, "code");
    const state = getQueryParam(req, "state");

    if (!code || !state) {
      res.status(400).json({ error: "code and state are required" });
      return;
    }

    try {
      const tokenResponse = await sdk.exchangeCodeForToken(code, state);
      const userInfo = await sdk.getUserInfo(tokenResponse.accessToken);

      if (!userInfo.openId) {
        res.status(400).json({ error: "openId missing from user info" });
        return;
      }

      await db.upsertUser({
        openId: userInfo.openId,
        name: userInfo.name || null,
        email: userInfo.email ?? null,
        loginMethod: userInfo.loginMethod ?? userInfo.platform ?? null,
        lastSignedIn: new Date(),
      });

      const sessionToken = await sdk.createSessionToken(userInfo.openId, {
        name: userInfo.name || "",
        expiresInMs: ONE_YEAR_MS,
      });

      const cookieOptions = getSessionCookieOptions(req);
      res.cookie(COOKIE_NAME, sessionToken, { ...cookieOptions, maxAge: ONE_YEAR_MS });

      res.redirect(302, "/");
    } catch (error) {
      console.error("[OAuth] Callback failed", error);
      const isDev = process.env.NODE_ENV !== "production";
      res.status(500).json({ 
        error: "OAuth callback failed",
        info: isDev && error instanceof Error ? error.message : undefined,
        stack: isDev && error instanceof Error ? error.stack : undefined
      });
    }
  });

  // Rota de bypass de login para ambiente de desenvolvimento local
  app.get("/api/oauth/local-login", async (req: Request, res: Response) => {
    try {
      const openId = "dev-usuario-padrao";
      const name = "Desenvolvedor Local";
      
      await db.upsertUser({
        openId: openId,
        name: name,
        email: "dev@localhost",
        loginMethod: "local",
        lastSignedIn: new Date(),
      });

      const sessionToken = await sdk.createSessionToken(openId, {
        name: name,
        expiresInMs: ONE_YEAR_MS,
      });

      const cookieOptions = getSessionCookieOptions(req);
      res.cookie(COOKIE_NAME, sessionToken, { ...cookieOptions, maxAge: ONE_YEAR_MS });

      res.redirect(302, "/");
    } catch (error) {
      console.error("[Local Login] Falha:", error);
      res.status(500).json({ error: "Local login failed" });
    }
  });
}
