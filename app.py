# VodiWalker launcher. Import main by its real module name for the optional plugins.
from main import HTTP_ONLY, PORT, app

if __name__ == "__main__":
    import uvicorn

    # The in-memory state and the on-disk/Postgres mirror use a single process.
    config = uvicorn.Config(app, host="0.0.0.0", port=PORT, workers=1)
    if not HTTP_ONLY:
        try:
            import tg_proxy

            config.load()
            if tg_proxy.attach_mux(app, config):
                print("tg-proxy: single-port mux enabled", flush=True)
        except Exception as exc:
            print(f"tg-proxy mux skipped: {exc}", flush=True)
    uvicorn.Server(config).run()
