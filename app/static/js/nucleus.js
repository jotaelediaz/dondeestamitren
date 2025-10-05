// app/static/js/nucleus.js
(function () {
    const COOKIE = "demt.nucleus";
    const LSK = "demt.nucleus";

    function getCookie(name) {
        const m = document.cookie.match(new RegExp("(^| )" + name + "=([^;]+)"));
        return m ? decodeURIComponent(m[2]) : null;
    }
    function setLS(v){ try{ localStorage.setItem(LSK, v || ""); }catch{} }
    function getLS(){ try{ return localStorage.getItem(LSK) || ""; }catch{ return ""; } }

    // Sync cookie -> localStorage
    const c = getCookie(COOKIE) || "";
    if (c && c !== getLS()) setLS(c);

    // Add nucleus to htmx requests
    document.body.addEventListener("htmx:configRequest", function (evt) {
        const slug = getLS();
        if (slug) evt.detail.headers["X-User-Nucleus"] = slug;
    });

    async function setNucleus(slug) {
        const res = await fetch("/prefs/nucleus", {
            method: "POST",
            headers: {"Content-Type":"application/json"},
            body: JSON.stringify({ slug })
        });
        if (!res.ok) throw new Error("No se pudo fijar el núcleo");
        const data = await res.json();
        setLS(data.slug);
        document.dispatchEvent(new CustomEvent("nucleus:changed", { detail: data }));
        return data;
    }

    async function clearNucleus() {
        await fetch("/prefs/nucleus", { method: "DELETE" });
        setLS("");
        document.dispatchEvent(new Event("nucleus:cleared"));
    }

    window.Nucleus = { set: setNucleus, clear: clearNucleus, get: getLS };
})();
