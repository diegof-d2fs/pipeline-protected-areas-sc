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

  // O diagrama é desenhado depois da abertura e muda a altura da página. Ao abrir um link com
  // âncora (#secao), a página é realinhada na seção certa depois do desenho e do carregamento.
  let userScrolled = false;
  ["wheel", "touchmove", "keydown"].forEach((ev) => window.addEventListener(ev, () => { userScrolled = true; }, { once: true, passive: true }));
  const realign = () => {
    if (userScrolled || !location.hash) return;
    const target = document.getElementById(decodeURIComponent(location.hash.slice(1)));
    if (target) target.scrollIntoView({ block: "start", behavior: "instant" });
  };
  renderDiagrams();
  pending.then(realign);
  window.addEventListener("load", realign);

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
  // Um título conta como "atual" quando chega à faixa logo abaixo da barra superior. A margem
  // (140 px) precisa ser maior que o deslocamento de rolagem do CSS (scroll-padding-top), senão
  // o sumário destaca a seção anterior à clicada.
  const ACTIVE_LINE = 140;
  let lockedUntil = 0;
  const onScroll = () => {
    document.querySelector(".to-top").classList.toggle("show", window.scrollY > 600);
    if (Date.now() < lockedUntil) return;
    let id = headings.length ? headings[0].id : null;
    for (const h of headings) {
      if (h.getBoundingClientRect().top - ACTIVE_LINE <= 0) id = h.id; else break;
    }
    if (id) activate(id);
  };
  window.addEventListener("scroll", onScroll, { passive: true });
  onScroll();
  // Ao clicar em qualquer link interno, o item correspondente do sumário é destacado na hora e
  // fica fixo enquanto a rolagem suave passa pelas seções intermediárias.
  document.addEventListener("click", (event) => {
    const a = event.target.closest('a[href^="#"]');
    if (!a) return;
    const id = decodeURIComponent(a.getAttribute("href").slice(1));
    if (!links.has(id)) return;
    lockedUntil = Date.now() + 1200;
    activate(id);
  });
  window.addEventListener("hashchange", () => {
    const id = decodeURIComponent(location.hash.slice(1));
    if (links.has(id)) { lockedUntil = Date.now() + 1200; activate(id); }
  });

  document.querySelector(".to-top").addEventListener("click", () => window.scrollTo({ top: 0 }));

  // Figuras em tela cheia, como nos slides da defesa: clique (ou Enter) na imagem abre.
  // Dentro: roda do mouse ou + / − dão zoom, arrastar move, duplo clique alterna o zoom,
  // 0 ou "Ajustar" volta ao tamanho da tela, e X, Esc ou clique fora fecham.
  const lb = document.createElement("div");
  lb.className = "lightbox";
  lb.setAttribute("role", "dialog");
  lb.setAttribute("aria-modal", "true");
  lb.setAttribute("aria-label", "Imagem em tela cheia");
  lb.innerHTML =
    '<div class="lb-bar"><span class="lb-cap"></span>' +
    '<button type="button" class="lb-out" aria-label="Diminuir zoom">−</button>' +
    '<span class="lb-pct">100%</span>' +
    '<button type="button" class="lb-in" aria-label="Aumentar zoom">+</button>' +
    '<button type="button" class="lb-fit">Ajustar</button>' +
    '<button type="button" class="lb-open">Abrir original</button>' +
    '<button type="button" class="lb-close" aria-label="Fechar (Esc)">✕</button></div>' +
    '<div class="lb-stage"><img alt="" draggable="false"></div>' +
    '<div class="lb-hint">Roda do mouse ou + / − para zoom · arraste para mover · duplo clique alterna o zoom · Esc ou ✕ para voltar ao guia</div>';
  document.body.appendChild(lb);
  const lbImg = lb.querySelector("img");
  const lbStage = lb.querySelector(".lb-stage");
  const lbPct = lb.querySelector(".lb-pct");
  let lastFocus = null;
  let z = 1, x = 0, y = 0, drag = null, moved = false;
  const apply = () => {
    if (z <= 1) { z = 1; x = 0; y = 0; }
    lbImg.style.transform = `translate(${x}px, ${y}px) scale(${z})`;
    lbStage.classList.toggle("zoomed", z > 1);
    lbPct.textContent = Math.round(z * 100) + "%";
  };
  const zoomAt = (factor, cx, cy) => {
    const nz = Math.max(1, Math.min(8, z * factor));
    if (cx !== undefined) {
      // mantém o ponto sob o cursor parado enquanto o zoom muda
      const r = lbStage.getBoundingClientRect();
      const px = cx - (r.left + r.width / 2), py = cy - (r.top + r.height / 2);
      x = px - (px - x) * (nz / z);
      y = py - (py - y) * (nz / z);
    }
    z = nz;
    apply();
  };
  const captionOf = (img) => {
    const p = img.closest("p");
    const next = p && p.nextElementSibling;
    if (next && next.tagName === "P" && next.querySelector("em")) return next.textContent.trim();
    return img.alt || "";
  };
  const openLb = (img) => {
    lastFocus = img;
    lbImg.src = img.currentSrc || img.src;
    lbImg.alt = img.alt;
    lb.querySelector(".lb-cap").textContent = captionOf(img);
    z = 1; apply();
    lb.classList.add("open");
    document.body.style.overflow = "hidden";
    lb.querySelector(".lb-close").focus();
  };
  const closeLb = () => {
    lb.classList.remove("open");
    document.body.style.overflow = "";
    if (lastFocus) lastFocus.focus();
  };

  // Selo "Ampliar" sobre cada imagem do guia, como nas figuras dos slides.
  document.querySelectorAll(".content img").forEach((img) => {
    const wrap = document.createElement("span");
    wrap.className = "zoomable";
    img.parentNode.insertBefore(wrap, img);
    wrap.appendChild(img);
    const badge = document.createElement("span");
    badge.className = "zoom-badge";
    badge.setAttribute("aria-hidden", "true");
    badge.textContent = "⤢ Ampliar";
    wrap.appendChild(badge);
    wrap.addEventListener("click", () => openLb(img));
    img.addEventListener("keydown", (e) => {
      if (e.key === "Enter" || e.key === " ") { e.preventDefault(); openLb(img); }
    });
  });

  lb.querySelector(".lb-close").addEventListener("click", closeLb);
  lb.querySelector(".lb-in").addEventListener("click", () => zoomAt(1.4));
  lb.querySelector(".lb-out").addEventListener("click", () => zoomAt(1 / 1.4));
  lb.querySelector(".lb-fit").addEventListener("click", () => { z = 1; apply(); });
  lb.querySelector(".lb-open").addEventListener("click", () => window.open(lbImg.src, "_blank", "noopener"));
  lbStage.addEventListener("wheel", (e) => { e.preventDefault(); zoomAt(e.deltaY < 0 ? 1.15 : 1 / 1.15, e.clientX, e.clientY); }, { passive: false });
  lbStage.addEventListener("dblclick", (e) => { if (z > 1) { z = 1; apply(); } else zoomAt(2.5, e.clientX, e.clientY); });
  lbStage.addEventListener("pointerdown", (e) => {
    moved = false;
    if (z <= 1) return;
    drag = { sx: e.clientX, sy: e.clientY, x, y };
    lbStage.setPointerCapture(e.pointerId);
    lbStage.classList.add("dragging");
  });
  lbStage.addEventListener("pointermove", (e) => {
    if (!drag) return;
    const dx = e.clientX - drag.sx, dy = e.clientY - drag.sy;
    if (Math.abs(dx) + Math.abs(dy) > 3) moved = true;
    x = drag.x + dx; y = drag.y + dy; apply();
  });
  const endDrag = () => { drag = null; lbStage.classList.remove("dragging"); };
  lbStage.addEventListener("pointerup", endDrag);
  lbStage.addEventListener("pointercancel", endDrag);
  lbStage.addEventListener("click", (e) => { if (!moved && e.target === lbStage && z === 1) closeLb(); });
  document.addEventListener("keydown", (e) => {
    if (!lb.classList.contains("open")) return;
    if (e.key === "Escape") closeLb();
    else if (e.key === "+" || e.key === "=") zoomAt(1.4);
    else if (e.key === "-") zoomAt(1 / 1.4);
    else if (e.key === "0") { z = 1; apply(); }
    else return;
    e.preventDefault();
  });
})();
