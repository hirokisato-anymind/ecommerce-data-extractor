import { NextResponse } from "next/server";
import type { NextRequest } from "next/server";

export function middleware(request: NextRequest) {
  const { pathname } = request.nextUrl;

  // Skip middleware for these paths
  if (
    pathname === "/login" ||
    pathname.startsWith("/api") ||
    pathname.startsWith("/_next") ||
    pathname === "/favicon.ico" ||
    /\.(?:png|jpg|jpeg|webp|svg|gif|ico|css|js|map)$/.test(pathname)
  ) {
    return NextResponse.next();
  }

  // TODO: ログイン機能は一時的に無効化中。有効化するには以下のコメントを外す
  // const sessionToken = request.cookies.get("session_token");
  // if (!sessionToken) {
  //   const loginUrl = new URL("/login", request.url);
  //   return NextResponse.redirect(loginUrl);
  // }

  return NextResponse.next();
}

export const config = {
  matcher: ["/((?!_next/static|_next/image|favicon.ico).*)"],
};
