import { describe, it, expect } from "vitest";
import { validatePayload, type NotificationPayload } from "../notification";
import { TRPCError } from "@trpc/server";

describe("validatePayload", () => {
  it("should return the payload with trimmed title and content when valid", () => {
    const payload: NotificationPayload = {
      title: "  Valid Title  ",
      content: "  Valid Content  ",
    };
    const result = validatePayload(payload);
    expect(result).toEqual({
      title: "Valid Title",
      content: "Valid Content",
    });
  });

  it("should throw a BAD_REQUEST TRPCError if title is missing or empty", () => {
    const payload = { content: "Valid Content" } as NotificationPayload;
    expect(() => validatePayload(payload)).toThrowError(
      new TRPCError({
        code: "BAD_REQUEST",
        message: "Notification title is required.",
      })
    );

    const emptyTitlePayload: NotificationPayload = { title: "   ", content: "Valid Content" };
    expect(() => validatePayload(emptyTitlePayload)).toThrowError(
      new TRPCError({
        code: "BAD_REQUEST",
        message: "Notification title is required.",
      })
    );
  });

  it("should throw a BAD_REQUEST TRPCError if content is missing or empty", () => {
    const payload = { title: "Valid Title" } as NotificationPayload;
    expect(() => validatePayload(payload)).toThrowError(
      new TRPCError({
        code: "BAD_REQUEST",
        message: "Notification content is required.",
      })
    );

    const emptyContentPayload: NotificationPayload = { title: "Valid Title", content: "   " };
    expect(() => validatePayload(emptyContentPayload)).toThrowError(
      new TRPCError({
        code: "BAD_REQUEST",
        message: "Notification content is required.",
      })
    );
  });

  it("should throw a BAD_REQUEST TRPCError if title exceeds max length", () => {
    const longTitle = "a".repeat(1201);
    const payload: NotificationPayload = { title: longTitle, content: "Valid Content" };
    expect(() => validatePayload(payload)).toThrowError(
      new TRPCError({
        code: "BAD_REQUEST",
        message: "Notification title must be at most 1200 characters.",
      })
    );
  });

  it("should throw a BAD_REQUEST TRPCError if content exceeds max length", () => {
    const longContent = "a".repeat(20001);
    const payload: NotificationPayload = { title: "Valid Title", content: longContent };
    expect(() => validatePayload(payload)).toThrowError(
      new TRPCError({
        code: "BAD_REQUEST",
        message: "Notification content must be at most 20000 characters.",
      })
    );
  });
});
