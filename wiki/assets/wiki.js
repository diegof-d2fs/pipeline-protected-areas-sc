// Comportamento da wiki: realce de código, botão de copiar, sumário que acompanha a leitura e mermaid.
(function () {
  // Tema: automático (segue o sistema), claro ou escuro. A escolha fica no navegador do leitor.
  const root = document.documentElement;
  const media = window.matchMedia("(prefers-color-scheme: dark)");
  const themeButton = document.querySelector(".theme");
  const labels = { auto: "🌓 Automático", light: "☀️ Claro", dark: "🌙 Escuro" };
  const order = ["auto", "light", "dark"];
  const isDark = () => root.dataset.theme === "dark" || (!root.dataset.theme && media.matches);
  const diagrams = [...document.querySelectorAll(".mermaid")].map((el) => [el, el.textContent]);

  // Renderizações em fila: trocar o tema durante um desenho não pode cruzar dois desenhos.
  let pending = Promise.resolve();
  const renderDiagrams = () => {
    if (!window.mermaid || !diagrams.length) return;
    pending = pending.then(async () => {
      mermaid.initialize({ startOnLoad: false, theme: isDark() ? "dark" : "default", securityLevel: "strict" });
      diagrams.forEach(([el, source]) => { el.removeAttribute("data-processed"); el.textContent = source; });
      await mermaid.run({ nodes: diagrams.map(([el]) => el) });
    }).catch((err) => console.error("mermaid:", err && err.message ? err.message : err));
  };
  const applyTheme = (choice) => {
    if (choice === "auto") delete root.dataset.theme; else root.dataset.theme = choice;
    themeButton.textContent = labels[choice];
    try { if (choice === "auto") localStorage.removeItem("wiki-tema"); else localStorage.setItem("wiki-tema", choice); } catch (err) {}
    renderDiagrams();
  };
  themeButton.addEventListener("click", () => {
    const current = root.dataset.theme || "auto";
    applyTheme(order[(order.indexOf(current) + 1) % order.length]);
  });
  media.addEventListener("change", () => { if (!root.dataset.theme) renderDiagrams(); });
  themeButton.textContent = labels[root.dataset.theme || "auto"];
  renderDiagrams();

  document.querySelectorAll("pre > code").forEach((code) => {
    if (window.hljs && !code.classList.contains("language-text")) hljs.highlightElement(code);
    const button = document.createElement("button");
    button.type = "button";
    button.className = "copy";
    button.textContent = "Copiar";
    button.addEventListener("click", async () => {
      try {
        await navigator.clipboard.writeText(code.innerText);
        button.textContent = "Copiado";
        button.classList.add("done");
      } catch (err) {
        button.textContent = "Selecione e copie";
      }
      setTimeout(() => { button.textContent = "Copiar"; button.classList.remove("done"); }, 1800);
    });
    code.parentElement.appendChild(button);
  });

  const sidebar = document.querySelector(".sidebar");
  const menu = document.querySelector(".menu");
  menu.addEventListener("click", () => {
    const open = sidebar.classList.toggle("open");
    menu.setAttribute("aria-expanded", String(open));
  });
  sidebar.addEventListener("click", (event) => {
    if (event.target.closest("a") && window.innerWidth <= 1000) {
      sidebar.classList.remove("open");
      menu.setAttribute("aria-expanded", "false");
    }
  });

  const links = new Map();
  document.querySelectorAll(".toc a").forEach((a) => links.set(decodeURIComponent(a.hash.slice(1)), a));
  const headings = [...document.querySelectorAll(".content h2[id], .content h3[id]")];
  let current = null;
  const activate = (id) => {
    const link = links.get(id);
    if (!link || link === current) return;
    if (current) current.classList.remove("active");
    document.querySelectorAll(".toc li.open").forEach((li) => li.classList.remove("open"));
    link.classList.add("active");
    let li = link.closest("li");
    while (li) { li.classList.add("open"); li = li.parentElement.closest("li"); }
    current = link;
  };
  const onScroll = () => {
    let id = headings.length ? headings[0].id : null;
    for (const h of headings) {
      if (h.getBoundingClientRect().top - 90 <= 0) id = h.id; else break;
    }
    if (id) activate(id);
    document.querySelector(".to-top").classList.toggle("show", window.scrollY > 600);
  };
  window.addEventListener("scroll", onScroll, { passive: true });
  onScroll();

  document.querySelector(".to-top").addEventListener("click", () => window.scrollTo({ top: 0 }));
})();
