import { useEffect, useState } from "react";

export default function App() {
  const [connection, setConnection] = useState("Checking backend…");

  useEffect(() => {
    const controller = new AbortController();
    fetch("/api/config", { signal: controller.signal })
      .then((response) => {
        if (!response.ok) throw new Error("Backend unavailable");
        return response.json();
      })
      .then(() => setConnection("Backend connected"))
      .catch((error) => {
        if (error.name !== "AbortError") setConnection("Backend unavailable");
      });
    return () => controller.abort();
  }, []);

  return (
    <main>
      <p className="eyebrow">Real-Time Analytics</p>
      <h1>Wikipedia, live</h1>
      <p>Explore changes across Wikipedia and its sister projects.</p>
      <section aria-label="Project setup">
        <h2>Project setup</h2>
        <p role="status">{connection}</p>
        <p>The live feed, statistics, and charts will arrive in the next implementation steps.</p>
      </section>
    </main>
  );
}
