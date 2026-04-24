(function () {
  const APP_ICON_SELECTOR = ".av-original-link-app-icon";
  const LINK_SELECTOR = "a.av-original-link";
  const URL_ATTR = "data-av-url";
  const POST_UID_ATTR = "data-av-post-uid";
  const MOBILE_USER_AGENT_RE = /android|iphone|ipad|ipod|mobile/i;
  const PHONE_DEVICE_MAX_DIMENSION_PX = 1024;
  const WEIBO_HOST_SUFFIXES = ["weibo.com", "weibo.cn"];
  const WEIBO_DETAIL_PATH_HEADS = new Set(["detail", "status"]);
  const XUEQIU_HOST_SUFFIX = "xueqiu.com";
  const XUEQIU_HTML_SUFFIX_RE = /\.html$/i;
  const XUEQIU_STOCK_PATH_HEAD = "S";

  function cleanText(value) {
    return String(value || "").trim();
  }

  function parseUrl(value) {
    const text = cleanText(value);
    if (!text) return null;
    try {
      return new URL(text);
    } catch (_error) {
      return null;
    }
  }

  function matchesHostSuffix(hostname, suffix) {
    const host = cleanText(hostname).toLowerCase();
    return Boolean(host) && (host === suffix || host.endsWith(`.${suffix}`));
  }

  function isWeiboHost(hostname) {
    return WEIBO_HOST_SUFFIXES.some((suffix) => matchesHostSuffix(hostname, suffix));
  }

  function isXueqiuHost(hostname) {
    return matchesHostSuffix(hostname, XUEQIU_HOST_SUFFIX);
  }

  function inferPlatformFromPostUid(postUid) {
    const lowered = cleanText(postUid).toLowerCase();
    if (lowered.startsWith("weibo:")) return "weibo";
    if (lowered.startsWith("xueqiu:")) return "xueqiu";
    return "";
  }

  function extractPlatformPostId(postUid) {
    const text = cleanText(postUid);
    if (!text) return "";
    const parts = text.split(":");
    const postId = cleanText(parts[parts.length - 1] || "");
    if (!postId) return "";
    const lowered = postId.toLowerCase();
    if (
      lowered.startsWith("http://") ||
      lowered.startsWith("https://") ||
      lowered.startsWith("linkhash:")
    ) {
      return "";
    }
    return postId;
  }

  function resolvePlatform(webUrl, postUid) {
    const platformFromPostUid = inferPlatformFromPostUid(postUid);
    if (platformFromPostUid) return platformFromPostUid;

    const parsed = parseUrl(webUrl);
    if (!parsed) return "";

    if (isWeiboHost(parsed.hostname)) return "weibo";
    if (isXueqiuHost(parsed.hostname)) return "xueqiu";
    return "";
  }

  function resolveWeiboPostId(webUrl, postUid) {
    const parsed = parseUrl(webUrl);
    if (parsed && isWeiboHost(parsed.hostname)) {
      const segments = parsed.pathname.split("/").filter(Boolean);
      if (
        segments.length >= 2 &&
        WEIBO_DETAIL_PATH_HEADS.has(cleanText(segments[0]).toLowerCase())
      ) {
        return cleanText(segments[1]);
      }
      if (segments.length >= 2 && /^\d+$/.test(cleanText(segments[0]))) {
        return cleanText(segments[1]);
      }
    }

    if (cleanText(postUid).toLowerCase().includes(":linkhash:")) {
      return "";
    }
    return extractPlatformPostId(postUid);
  }

  function resolveXueqiuUserPostPath(segments) {
    if (segments.length < 2) return "";
    const userId = cleanText(segments[0]);
    const postId = cleanText(segments[1]).replace(XUEQIU_HTML_SUFFIX_RE, "");
    if (!userId || !/^\d+$/.test(postId)) return "";
    return `${userId}/${postId}`;
  }

  function resolveXueqiuStockPostPath(segments) {
    if (segments.length < 3) return "";
    const stockSymbol = cleanText(segments[1]);
    const postId = cleanText(segments[2]).replace(XUEQIU_HTML_SUFFIX_RE, "");
    if (!stockSymbol || !/^\d+$/.test(postId)) return "";
    return `${XUEQIU_STOCK_PATH_HEAD}/${stockSymbol}/${postId}`;
  }

  function resolveXueqiuDeepLinkPath(webUrl) {
    const parsed = parseUrl(webUrl);
    if (!parsed || !isXueqiuHost(parsed.hostname)) return "";

    const segments = parsed.pathname.split("/").filter(Boolean);
    if (!segments.length) return "";
    if (cleanText(segments[0]).toUpperCase() === XUEQIU_STOCK_PATH_HEAD) {
      return resolveXueqiuStockPostPath(segments);
    }
    return resolveXueqiuUserPostPath(segments);
  }

  function buildDeepLink(webUrl, postUid) {
    const platform = resolvePlatform(webUrl, postUid);
    if (platform === "weibo") {
      const postId = resolveWeiboPostId(webUrl, postUid);
      return postId ? `sinaweibo://detail?mblogid=${postId}` : "";
    }

    if (platform !== "xueqiu") return "";

    const deepLinkPath = resolveXueqiuDeepLinkPath(webUrl);
    return deepLinkPath ? `xueqiu://${deepLinkPath}` : "";
  }

  function hasCoarsePointer() {
    return typeof window.matchMedia === "function" &&
      window.matchMedia("(pointer: coarse)").matches;
  }

  function hasTouchInput() {
    return Number(navigator.maxTouchPoints || 0) > 0 || hasCoarsePointer();
  }

  function collectDeviceSides() {
    const screenWidth = Number(window.screen?.width || 0);
    const screenHeight = Number(window.screen?.height || 0);
    const viewportWidth = Number(window.innerWidth || 0);
    const viewportHeight = Number(window.innerHeight || 0);
    return [
      screenWidth,
      screenHeight,
      viewportWidth,
      viewportHeight,
    ].filter((value) => value > 0);
  }

  function hasLikelyPhoneScreen() {
    const candidateSides = collectDeviceSides();
    if (!candidateSides.length) return false;

    const shortestSide = Math.min(...candidateSides);
    return shortestSide <= PHONE_DEVICE_MAX_DIMENSION_PX;
  }

  function isLikelyPhoneDevice() {
    if (navigator.userAgentData && navigator.userAgentData.mobile === true) {
      return true;
    }
    if (MOBILE_USER_AGENT_RE.test(navigator.userAgent || "")) {
      return true;
    }

    if (!hasTouchInput()) return false;
    if (!hasLikelyPhoneScreen()) return false;
    return true;
  }

  function openDeepLinkInCurrentPage(deepLink) {
    window.location.href = deepLink;
  }

  function syncLinkHref(link, href) {
    if (!(link instanceof HTMLAnchorElement)) return;
    const nextHref = cleanText(href);
    if (!nextHref) return;
    if (cleanText(link.getAttribute("href")) === nextHref) return;
    link.setAttribute("href", nextHref);
  }

  function syncLinkBehavior(link, webUrl, deepLink, shouldTryAppOpen) {
    const nextHref = shouldTryAppOpen ? deepLink : webUrl;
    syncLinkHref(link, nextHref);
  }

  function setAppIconVisibility(link, visible) {
    if (!(link instanceof HTMLAnchorElement)) return;
    const icon = link.querySelector(APP_ICON_SELECTOR);
    if (!(icon instanceof HTMLElement)) return;
    icon.style.display = visible ? "inline" : "none";
  }

  function syncLinkIndicator(link, shouldShowAppIcon) {
    setAppIconVisibility(link, shouldShowAppIcon);
  }

  function resolveLinkOpenState(link) {
    const webUrl = cleanText(link.getAttribute(URL_ATTR) || link.getAttribute("href"));
    const postUid = cleanText(link.getAttribute(POST_UID_ATTR));
    const deepLink = buildDeepLink(webUrl, postUid);
    const shouldTryAppOpen = isLikelyPhoneDevice() && Boolean(deepLink);
    syncLinkBehavior(link, webUrl, deepLink, shouldTryAppOpen);
    syncLinkIndicator(link, shouldTryAppOpen);
    return { webUrl, deepLink, shouldTryAppOpen };
  }

  function syncAllLinkIndicators() {
    const links = document.querySelectorAll(LINK_SELECTOR);
    for (const link of links) {
      if (!(link instanceof HTMLAnchorElement)) continue;
      resolveLinkOpenState(link);
    }
  }

  function installLinkIndicatorObserver() {
    if (window.__avOriginalLinkObserverInit === true) {
      return;
    }
    if (typeof MutationObserver !== "function") {
      syncAllLinkIndicators();
      return;
    }

    const startObserver = () => {
      if (!(document.body instanceof HTMLBodyElement)) {
        syncAllLinkIndicators();
        return;
      }
      syncAllLinkIndicators();
      const observer = new MutationObserver(() => {
        syncAllLinkIndicators();
      });
      observer.observe(document.body, {
        childList: true,
        subtree: true,
        attributes: true,
        attributeFilter: [URL_ATTR, POST_UID_ATTR],
      });
    };

    window.__avOriginalLinkObserverInit = true;
    if (document.readyState === "loading") {
      document.addEventListener("DOMContentLoaded", startObserver, { once: true });
      return;
    }
    startObserver();
  }

  function resolveEventElement(event) {
    const path = typeof event.composedPath === "function" ? event.composedPath() : [];
    for (const entry of path) {
      if (!(entry instanceof Element)) continue;
      const matchedLink = entry.closest(LINK_SELECTOR);
      if (matchedLink) return matchedLink;
    }

    const target = event.target;
    if (target instanceof Element) {
      return target.closest(LINK_SELECTOR);
    }
    if (target && target.parentElement instanceof Element) {
      return target.parentElement.closest(LINK_SELECTOR);
    }
    return null;
  }

  function handleOriginalLinkClick(event) {
    if (event.defaultPrevented) return;
    if (typeof event.button === "number" && event.button !== 0) return;
    if (event.metaKey || event.ctrlKey || event.shiftKey || event.altKey) return;

    const link = resolveEventElement(event);
    if (!link) return;

    const { webUrl, deepLink, shouldTryAppOpen } = resolveLinkOpenState(link);
    if (!webUrl || !deepLink) return;

    if (!shouldTryAppOpen) {
      return;
    }

    event.preventDefault();
    openDeepLinkInCurrentPage(deepLink);
  }

  if (window.__avOriginalLinkInit === true) {
    return;
  }
  window.__avOriginalLinkInit = true;
  document.addEventListener("click", handleOriginalLinkClick, true);
  installLinkIndicatorObserver();
  window.addEventListener("pageshow", syncAllLinkIndicators, true);
  window.addEventListener("resize", syncAllLinkIndicators, true);
})();
