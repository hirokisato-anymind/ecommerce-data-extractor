import { NextResponse } from "next/server";
import type { NextRequest } from "next/server";

export function middleware(request: NextRequest) {
  const { pathname } = request.nextUrl;

  // Next.js 内部パス・静的ファイルはスキップ
  if (
    pathname.startsWith("/_next") ||
    pathname.startsWith("/api") ||
    pathname === "/favicon.ico" ||
    /\.(?:png|jpg|jpeg|webp|svg|gif|ico|css|js|map)$/.test(pathname)
  ) {
    return NextResponse.next();
  }

  // ルート以外のパスは全て `/` にリダイレクト
  if (pathname !== "/") {
    return NextResponse.redirect(new URL("/", request.url));
  }

  return NextResponse.next();
}

export const config = {
  matcher: ["/((?!_next/static|_next/image|favicon.ico).*)"],
};
