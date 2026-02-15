# CHANGELOG

<!-- version list -->

## v1.2.0 (2026-02-15)

### Bug Fixes

- Avoid file backend info read/write race
  ([`92ff535`](https://github.com/elohmeier/litestar-tus/commit/92ff53535e3e4aad5f2a3671ffa7e9410d0da3a9))

- Normalize s3 etags for conditional writes
  ([`635753c`](https://github.com/elohmeier/litestar-tus/commit/635753c873d7a2e2df9c6d292636695122a74d01))

### Chores

- Apply ruff fixes
  ([`f94a052`](https://github.com/elohmeier/litestar-tus/commit/f94a052a69fab0b9b370ad3c1ef2c67d6443c9c4))

- Fix ruff B904 and SIM102 violations
  ([`2c1c226`](https://github.com/elohmeier/litestar-tus/commit/2c1c22633cdd53db4c1f4f1b0b493838dd5d7950))

### Features

- Add standalone server module and Docker image
  ([`f9a4fe9`](https://github.com/elohmeier/litestar-tus/commit/f9a4fe97ddfd173c221629b4d82dca3c615085ba))

- Implement TUS concatenation extension
  ([`db78aa4`](https://github.com/elohmeier/litestar-tus/commit/db78aa4cf2c969f711eb394d0158ca91fbac7b1a))

- Set up performance test
  ([`659cdbd`](https://github.com/elohmeier/litestar-tus/commit/659cdbd0e152ebd98afbcff9e1d88779835ef71a))


## v1.1.1 (2026-01-31)

### Bug Fixes

- Skip tus events with no listeners
  ([`3b57016`](https://github.com/elohmeier/litestar-tus/commit/3b57016e75332aa4974b1c67bf400dad1e59c1e3))


## v1.1.0 (2026-01-31)

### Features

- Add metadata override hook
  ([`359861d`](https://github.com/elohmeier/litestar-tus/commit/359861da12fbb39452bcf6b19c9a4ee27eb67734))


## v1.0.0 (2026-01-31)

### Bug Fixes

- Log tus event handler failures
  ([`7cbb24e`](https://github.com/elohmeier/litestar-tus/commit/7cbb24e72e4a0905f1bf15b398c703b7f80be338))

- Replace full-stream buffering with rolling buffer in S3 backend
  ([`05d5368`](https://github.com/elohmeier/litestar-tus/commit/05d536856c53e675ec1529146c109af27c04df3f))

- Rollback file writes on checksum failure
  ([`9f0310b`](https://github.com/elohmeier/litestar-tus/commit/9f0310b6e3607ec4f662493e87263e17c734d2d3))

- Stream file backend writes
  ([`0214937`](https://github.com/elohmeier/litestar-tus/commit/0214937912761710840780403f0006428bcb12ae))

### Chores

- Bump version to 1.0.0
  ([`910f7b7`](https://github.com/elohmeier/litestar-tus/commit/910f7b7a8878f22a916c9591e772847e3ecd8ab3))

### Features

- Add checksum verification, expiration enforcement, and content-length validation
  ([`706349b`](https://github.com/elohmeier/litestar-tus/commit/706349b6d7705ea9cfa503f084906e010d5e38ea))

- Add optimistic locking via S3 If-Match ETags
  ([`8179f79`](https://github.com/elohmeier/litestar-tus/commit/8179f798fea604195126c277530d3ebd52dc657a))


## v0.1.0 (2026-01-31)

- Initial Release
