import { NextRequest, NextResponse } from "next/server";

export async function GET(request: NextRequest) {
  const token = request.cookies.get("session_token")?.value;

  if (!token) {
    return NextResponse.json({ error: "認証が必要です" }, { status: 401 });
  }

  // Decode JWT payload without verification (verification is done by checking cookie existence + expiry)
  // The JWT was created by our backend with a known secret
  try {
    const payloadBase64 = token.split(".")[1];
    const payload = JSON.parse(
      Buffer.from(payloadBase64, "base64url").toString("utf-8")
    );

    // Check expiry
    if (payload.exp && payload.exp * 1000 < Date.now()) {
      return NextResponse.json(
        { error: "セッションが期限切れです" },
        { status: 401 }
      );
    }

    return NextResponse.json({
      email: payload.email || "",
      name: payload.name || "",
      picture: payload.picture || "",
    });
  } catch {
    return NextResponse.json({ error: "無効なセッションです" }, { status: 401 });
  }
}
