# Local Firebase ID token issuer

Add an opt-in issuer to the existing listener so generic clients can exercise
normal Firebase ID token verification without contacting Google. No consumer
code, OAuth flow, persistent state or production credentials belong here.

The routing layer selects exact raw `GET
/robot/v1/metadata/x509/securetoken@system.gserviceaccount.com` and `POST /mint`
only when enabled. Targets, signing scopes and bucket hosts keep their existing
priority. Classification completes before body collection; all other paths and
methods keep their current service owner. No registry or dispatch abstraction
is needed.

One immutable, Arc-shared service owns one OS-random RSA-2048 signing key and
self-signed x509 certificate. RSA generation uses `rsa`; rcgen imports the key
and signs the certificate with its existing ring backend; jsonwebtoken signs
RS256 ID tokens with its Rust crypto backend. No external crypto executable or
new service is needed. The certificate fingerprint supplies the kid, and the
public map is stable until restart. Keys and tokens are never logged or written
to disk. Key export is deliberately omitted.

Minting validates bounded JSON before signing, requires a project (request or
config), subject and email, and constructs Google's issuer/audience/identity
claims. Typed fields own standard claims; extra claims cannot shadow them.
Explicit timestamps and header kid allow negative verification scenarios.
Named expired, bad-kid and wrong-audience effects and their conflict validation
live in `firebase/faults.rs`. Shared scheduling/counters and AWS `FaultRule`
compatibility are untouched. Only the new optional service/config export extends
the library API.

Real-loopback tests independently verify fetched certificate signatures and
claim validation using jsonwebtoken; reject invalid variants; exercise input
limits; and prove AWS, S3, SSM and Responses coexist. Withheld-body requests
expose routing regressions. Mutations to kid, issuer and expiry must fail those
tests. A built-binary run additionally checks a consumer-style offline verifier.

Limits: synthetic local users, one static key per instance, no Google key
rotation, no authentication on mint, no refresh tokens, no OAuth/Identity Toolkit.
Clients must clear cached certificates after restarting the issuer. The CLI
continues to bind loopback only.
