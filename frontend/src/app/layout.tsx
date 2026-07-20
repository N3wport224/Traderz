import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "Traderz — Multi-Engine Trading Dashboard",
  description: "Real-time day trading and swing trading signal dashboard",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en" className="h-full antialiased">
      <body className="min-h-full flex flex-col bg-black">{children}</body>
    </html>
  );
}
