import { redirect } from "next/navigation";

export default function Home() {
  // The proxy redirects authenticated users to /dashboard and others to /login.
  redirect("/dashboard");
}
