# Mighty Card Game

[![Github Action](https://img.shields.io/github/workflow/status/buttercrab/web-mighty/build?style=flat-square)](https://github.com/buttercrab/web-mighty/actions)
[![Codecov](https://img.shields.io/codecov/c/github/buttercrab/web-mighty?style=flat-square)](https://codecov.io/gh/buttercrab/web-mighty)
[![LICENSE](https://img.shields.io/github/license/buttercrab/web-mighty?style=flat-square)](https://github.com/buttercrab/web-mighty/blob/master/LICENSE)

## Run your own server (WIP)

Python script will guide you to run from docker/native

```shell script
pip3 install -r requirements.txt
python3 util/start_server.py
```

## Manually start your server

1. [install rust](https://www.rust-lang.org/tools/install)

1. [install wasm-pack](https://rustwasm.github.io/wasm-pack/installer/)

1. [install sass](https://sass-lang.com/install)

1. [install bulma](https://bulma.io/): download latest version and unzip to `public/static/res/bulma`

1. convert `public/static/res/scss/style.scss` to `public/static/res/css/style.css` using `sass`

1. [install](https://www.postgresql.org/download/) and start your postgresql server

1. make `server/.env` file and fill
   with [environment values](https://github.com/buttercrab/web-mighty/blob/master/server/README.md)

1. run `cd public && wasm-pack build --target web --release`

1. build server

    1) if you want https:

       run `cargo install --features https --root build --path server`

    1) if you want only http:

       run `cargo install --root build --path server`

1. make `server.toml` based from `server.sample.toml`

1. run `./build/bin/server`