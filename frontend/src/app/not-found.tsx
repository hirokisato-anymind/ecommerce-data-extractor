"use client";

import { useEffect } from "react";
import { useRouter } from "next/navigation";

export default function NotFound() {
  const router = useRouter();

  useEffect(() => {
    router.replace("/");
  }, [router]);

  return (
    <div className="h-screen flex items-center justify-center bg-[#f4f6f9]">
      <div className="text-center">
        <p className="text-sm text-slate-500">リダイレクト中...</p>
      </div>
    </div>
  );
}
