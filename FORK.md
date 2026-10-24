# About this fork

Fork of [danielcherubini/elegoo-homeassistant](https://github.com/danielcherubini/elegoo-homeassistant).
It adds a slicer proxy for V1 (MQTT) resin printers such as the Mars 4 Ultra:
Home Assistant keeps the printer's single MQTT connection and offers itself to
ChiTuBox as the printer, so both can use it at the same time. The feature is
meant to go upstream as a pull request.

## Branches

| Branch | Content |
|---|---|
| `main` | Upstream `main`, unchanged; fast-forwarded every night |
| `feat/v1-slicer-proxy` | The feature on top of `main`; the upstream pull request |
| `stable` (default) | Latest upstream release plus the feature. Updated only after lint and tests pass |

Install `stable` through HACS as a custom repository
(`https://github.com/robertzaage/elegoo-homeassistant`, category Integration).
Remove the upstream repository from HACS first; both use the domain
`elegoo_printer`, so existing printers and entities stay.

## Sync

`.github/workflows/sync-upstream.yml` runs every night: it fast-forwards
`main`, merges each new upstream release into `stable`, runs upstream's lint
and tests, and only then pushes. A merge conflict or a failing check stops the
run before anything is pushed. Feature changes reach `stable` only by an
explicit merge, after they were tested on a printer.

The workflows inherited from upstream (releases, review bots) stay disabled in
this fork.
