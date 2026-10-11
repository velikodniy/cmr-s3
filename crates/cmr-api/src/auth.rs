use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use futures_util::{
    FutureExt,
    future::{BoxFuture, Shared},
};

use serde::Deserialize;
use tokio::time::Instant;
use url::Url;

use crate::{Error, Result, defaults, protocol, transport};

pub(crate) struct Authentication {
    source: Arc<TokenRequest>,
    state: Mutex<State>,
}

struct TokenRequest {
    login: String,
    password: String,
    http: reqwest::Client,
    endpoint: Url,
}

#[derive(Default)]
struct State {
    current: Option<Arc<Token>>,
    rejected: bool,
    pending: Option<Shared<BoxFuture<'static, Result<Arc<Token>>>>>,
}

pub(crate) struct Token {
    pub(crate) access: String,
    refresh: String,
    refresh_at: Instant,
}

#[derive(Deserialize)]
struct TokenReply {
    access_token: String,
    refresh_token: Option<String>,
    token_type: String,
    expires_in: u64,
}

impl Authentication {
    pub(crate) fn new(
        login: String,
        password: String,
        http: reqwest::Client,
        endpoint: Url,
    ) -> Result<Self> {
        if login.is_empty() || password.is_empty() {
            return Err(Error::InvalidCredentials);
        }
        Ok(Self {
            source: Arc::new(TokenRequest {
                login,
                password,
                http,
                endpoint,
            }),
            state: Mutex::new(State::default()),
        })
    }

    pub(crate) fn reject(&self, token: &Arc<Token>) {
        let mut state = self.state.lock().unwrap();
        if state
            .current
            .as_ref()
            .is_some_and(|current| Arc::ptr_eq(current, token))
        {
            state.rejected = true;
        }
    }

    pub(crate) async fn token(&self) -> Result<Arc<Token>> {
        let pending = {
            let mut state = self.state.lock().unwrap();
            if let Some(current) = &state.current
                && !state.rejected
                && Instant::now() < current.refresh_at
            {
                return Ok(current.clone());
            }
            if let Some(pending) = &state.pending {
                pending.clone()
            } else {
                let source = self.source.clone();
                let previous = state.current.clone();
                let pending = async move { source.refresh(previous).await }
                    .boxed()
                    .shared();
                state.pending = Some(pending.clone());
                pending
            }
        };
        let result = pending.clone().await;
        let mut state = self.state.lock().unwrap();
        // A late waiter must not replace a newer refresh or undo a rejection.
        if state
            .pending
            .as_ref()
            .is_some_and(|current| current.ptr_eq(&pending))
        {
            state.pending = None;
            if let Ok(token) = &result {
                state.current = Some(token.clone());
                state.rejected = false;
            }
        }
        result
    }
}

impl TokenRequest {
    async fn refresh(&self, previous: Option<Arc<Token>>) -> Result<Arc<Token>> {
        if let Some(current) = previous {
            let form = [
                ("client_id", protocol::CLIENT_ID),
                ("grant_type", "refresh_token"),
                ("refresh_token", &current.refresh),
            ];
            let started_at = Instant::now();
            match self.request(&form).await {
                Ok(reply) => {
                    return Token::from_reply(reply, Some(&current.refresh), started_at)
                        .map(Arc::new);
                }
                Err(Error::Http(400 | 401 | 403)) => {}
                Err(error) => return Err(error),
            }
        }
        let form = [
            ("client_id", protocol::CLIENT_ID),
            ("grant_type", "password"),
            ("username", &self.login),
            ("password", &self.password),
        ];
        let started_at = Instant::now();
        let reply = self.request(&form).await.map_err(|error| match error {
            Error::Http(400 | 401 | 403) => Error::Authentication,
            other => other,
        })?;
        Token::from_reply(reply, None, started_at).map(Arc::new)
    }

    async fn request(&self, form: &[(&str, &str)]) -> Result<TokenReply> {
        let request = self
            .http
            .post(self.endpoint.clone())
            .form(form)
            .timeout(defaults::CONTROL_TIMEOUT);
        let bytes = transport::control_request(request).await?;
        serde_json::from_slice(&bytes).map_err(|_| Error::Protocol("the token response is invalid"))
    }
}

impl Token {
    fn from_reply(
        reply: TokenReply,
        previous_refresh: Option<&str>,
        started_at: Instant,
    ) -> Result<Self> {
        if reply.access_token.is_empty()
            || !reply.token_type.eq_ignore_ascii_case("Bearer")
            || reply.expires_in == 0
        {
            return Err(Error::Protocol("the token fields are invalid"));
        }
        let refresh = reply
            .refresh_token
            .or_else(|| previous_refresh.map(str::to_owned))
            .filter(|value| !value.is_empty())
            .ok_or(Error::Protocol("the refresh token is absent"))?;
        let lifetime = Duration::from_secs(reply.expires_in);
        let margin = defaults::TOKEN_REFRESH_MARGIN.min(lifetime / 2);
        let refresh_at = started_at
            .checked_add(lifetime - margin)
            .ok_or(Error::Protocol("the token lifetime is too large"))?;
        Ok(Self {
            access: reply.access_token,
            refresh,
            refresh_at,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use httpmock::{HttpMockResponse, MockServer};
    use proptest::prelude::*;
    use std::{
        collections::{BTreeMap, VecDeque},
        thread,
    };

    const LOGIN: &[(&str, &str)] = &[
        ("client_id", "cloud-win"),
        ("grant_type", "password"),
        ("username", "login+name@example.com"),
        ("password", "password & secret"),
    ];
    const REFRESH: &[(&str, &str)] = &[
        ("client_id", "cloud-win"),
        ("grant_type", "refresh_token"),
        ("refresh_token", "refresh"),
    ];

    struct Exchange {
        form: &'static [(&'static str, &'static str)],
        status: u16,
        body: String,
        delay: Duration,
    }

    impl Exchange {
        fn new(form: &'static [(&'static str, &'static str)], status: u16, body: &str) -> Self {
            Self {
                form,
                status,
                body: body.into(),
                delay: Duration::ZERO,
            }
        }
    }

    fn token_body(access: &str, refresh: Option<&str>, lifetime: u64) -> String {
        let mut body = serde_json::json!({
            "access_token": access, "token_type": "Bearer", "expires_in": lifetime,
        });
        if let Some(refresh) = refresh {
            body["refresh_token"] = refresh.into();
        }
        body.to_string()
    }

    struct TokenServer {
        endpoint: Url,
        http: reqwest::Client,
        server: MockServer,
        remaining: Arc<Mutex<VecDeque<Exchange>>>,
    }

    impl TokenServer {
        fn start(exchanges: Vec<Exchange>) -> Self {
            let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
            let remaining = Arc::new(Mutex::new(VecDeque::from(exchanges)));
            let exchanges = remaining.clone();
            let server = MockServer::start();
            server.mock(|when, then| {
                when.method("POST")
                    .path("/token")
                    .header("content-type", "application/x-www-form-urlencoded");
                then.respond_with(move |request| {
                    let exchange = exchanges
                        .lock()
                        .unwrap()
                        .pop_front()
                        .expect("unexpected token request");
                    let form: BTreeMap<_, _> = url::form_urlencoded::parse(request.body_ref())
                        .into_owned()
                        .collect();
                    let expected = exchange
                        .form
                        .iter()
                        .map(|(key, value)| (key.to_string(), value.to_string()))
                        .collect();
                    assert_eq!(form, expected);
                    thread::sleep(exchange.delay);
                    HttpMockResponse::builder()
                        .status(exchange.status)
                        .body(exchange.body)
                        .build()
                });
            });
            Self {
                endpoint: Url::parse(&server.url("/token")).unwrap(),
                http: reqwest::Client::builder()
                    .no_proxy()
                    .danger_accept_invalid_certs(true)
                    .retry(reqwest::retry::never())
                    .build()
                    .unwrap(),
                server,
                remaining,
            }
        }

        fn authentication(&self) -> Authentication {
            Authentication::new(
                "login+name@example.com".into(),
                "password & secret".into(),
                self.http.clone(),
                self.endpoint.clone(),
            )
            .unwrap()
        }

        fn finish(self) {
            assert!(self.remaining.lock().unwrap().is_empty());
            drop(self.server);
        }
    }

    #[tokio::test]
    async fn tokens_are_cached_until_the_refresh_deadline() {
        let server = TokenServer::start(vec![
            Exchange::new(LOGIN, 200, &token_body("first", Some("refresh"), 3600)),
            Exchange::new(REFRESH, 200, &token_body("second", None, 3600)),
        ]);
        let auth = server.authentication();
        let first = auth.token().await.unwrap();
        assert!(Arc::ptr_eq(&first, &auth.token().await.unwrap()));

        tokio::time::pause();
        tokio::time::advance(Duration::from_secs(3600)).await;
        tokio::time::resume();
        let second = auth.token().await.unwrap();
        assert_eq!(second.access, "second");
        assert_eq!(second.refresh, "refresh");
        assert!(!Arc::ptr_eq(&first, &second));
        assert!(Arc::ptr_eq(&second, &auth.token().await.unwrap()));
        server.finish();
    }

    #[tokio::test]
    async fn rejected_refreshes_fall_back_to_password_login() {
        for status in [400, 401, 403] {
            let server = TokenServer::start(vec![
                Exchange::new(LOGIN, 200, &token_body("first", Some("refresh"), 3600)),
                Exchange::new(REFRESH, status, "rejected"),
                Exchange::new(LOGIN, 200, &token_body("second", Some("replacement"), 3600)),
            ]);
            let auth = server.authentication();
            let first = auth.token().await.unwrap();
            auth.reject(&first);
            let second = auth.token().await.unwrap();
            assert_eq!(second.access, "second");
            assert_eq!(second.refresh, "replacement");
            assert!(Arc::ptr_eq(&second, &auth.token().await.unwrap()));
            server.finish();
        }
    }

    #[tokio::test]
    async fn password_fallback_starts_a_new_token_lifetime() {
        let mut rejected = Exchange::new(REFRESH, 400, "rejected");
        rejected.delay = Duration::from_millis(600);
        let server = TokenServer::start(vec![
            Exchange::new(LOGIN, 200, &token_body("first", Some("refresh"), 3600)),
            rejected,
            Exchange::new(LOGIN, 200, &token_body("second", Some("replacement"), 1)),
        ]);
        let auth = server.authentication();
        let first = auth.token().await.unwrap();
        auth.reject(&first);
        let started = Instant::now();
        let second = auth.token().await.unwrap();
        // The failed refresh takes 600 ms. The new token refreshes after another 500 ms.
        assert!(second.refresh_at >= started + Duration::from_millis(1100));
        server.finish();
    }

    #[tokio::test]
    async fn failed_refreshes_preserve_credentials_for_the_next_attempt() {
        for (status, body) in [
            (503, "unavailable".to_owned()),
            (200, "not JSON".to_owned()),
            (200, token_body("", Some("replacement"), 3600)),
            (200, token_body("second", Some(""), 3600)),
        ] {
            let mut exchanges = vec![Exchange::new(
                LOGIN,
                200,
                &token_body("first", Some("refresh"), 3600),
            )];
            let attempts = if status == 503 {
                defaults::MAX_ATTEMPTS
            } else {
                1
            };
            exchanges.extend((0..attempts).map(|_| Exchange::new(REFRESH, status, &body)));
            exchanges.push(Exchange::new(
                REFRESH,
                200,
                &token_body("second", None, 3600),
            ));
            let server = TokenServer::start(exchanges);
            let auth = server.authentication();
            let first = auth.token().await.unwrap();
            auth.reject(&first);
            let error = auth.token().await.err().unwrap();
            if status == 503 {
                assert!(matches!(error, Error::Http(503)));
            } else {
                assert!(matches!(error, Error::Protocol(_)));
            }
            {
                let state = auth.state.lock().unwrap();
                assert!(Arc::ptr_eq(state.current.as_ref().unwrap(), &first));
                assert!(state.rejected);
            }
            let second = auth.token().await.unwrap();
            assert_eq!(second.access, "second");
            assert_eq!(second.refresh, "refresh");
            assert!(Arc::ptr_eq(&second, &auth.token().await.unwrap()));
            server.finish();
        }
    }

    #[tokio::test]
    async fn password_failures_do_not_install_or_reuse_credentials() {
        for (status, body) in [
            (400, "rejected".to_owned()),
            (401, "rejected".to_owned()),
            (403, "rejected".to_owned()),
            (200, token_body("second", None, 3600)),
        ] {
            let server = TokenServer::start(vec![
                Exchange::new(LOGIN, 200, &token_body("first", Some("refresh"), 3600)),
                Exchange::new(REFRESH, 400, "rejected"),
                Exchange::new(LOGIN, status, &body),
                Exchange::new(REFRESH, 200, &token_body("third", None, 3600)),
            ]);
            let auth = server.authentication();
            let first = auth.token().await.unwrap();
            auth.reject(&first);
            let error = auth.token().await.err().unwrap();
            if status == 200 {
                assert!(matches!(error, Error::Protocol(_)));
            } else {
                assert!(matches!(error, Error::Authentication));
            }
            let third = auth.token().await.unwrap();
            assert_eq!(third.access, "third");
            assert_eq!(third.refresh, "refresh");
            server.finish();
        }
    }

    #[tokio::test]
    async fn concurrent_callers_share_login_and_refresh_requests() {
        let server = TokenServer::start(vec![
            Exchange::new(LOGIN, 200, &token_body("first", Some("refresh"), 3600)),
            Exchange::new(
                REFRESH,
                200,
                &token_body("second", Some("replacement"), 3600),
            ),
        ]);
        let auth = server.authentication();
        for expected in ["first", "second"] {
            let results = futures_util::future::join_all((0..8).map(|_| auth.token())).await;
            let tokens: Vec<_> = results.into_iter().map(Result::unwrap).collect();
            for token in &tokens {
                assert_eq!(token.access, expected);
                assert!(Arc::ptr_eq(token, &tokens[0]));
            }
            auth.reject(&tokens[0]);
        }
        server.finish();
    }

    #[tokio::test]
    async fn transient_token_requests_are_retried() {
        let server = TokenServer::start(vec![
            Exchange::new(LOGIN, 503, "unavailable"),
            Exchange::new(LOGIN, 200, &token_body("first", Some("refresh"), 3600)),
        ]);
        let auth = server.authentication();
        assert_eq!(auth.token().await.unwrap().access, "first");
        server.finish();
    }

    #[tokio::test]
    async fn concurrent_refresh_failures_share_one_budget_and_preserve_the_refresh_token() {
        let mut exchanges = vec![Exchange::new(
            LOGIN,
            200,
            &token_body("first", Some("refresh"), 3600),
        )];
        exchanges.extend((0..defaults::MAX_ATTEMPTS).map(|_| {
            let mut failure = Exchange::new(REFRESH, 503, "unavailable");
            failure.delay = Duration::from_millis(25);
            failure
        }));
        exchanges.push(Exchange::new(
            REFRESH,
            200,
            &token_body("second", None, 3600),
        ));
        let server = TokenServer::start(exchanges);
        let auth = server.authentication();
        let first = auth.token().await.unwrap();
        auth.reject(&first);
        let results = futures_util::future::join_all((0..8).map(|_| auth.token())).await;
        assert!(
            results
                .iter()
                .all(|result| matches!(result, Err(Error::Http(503))))
        );
        assert_eq!(auth.token().await.unwrap().access, "second");
        server.finish();
    }

    #[tokio::test]
    async fn cancelled_token_callers_do_not_strand_the_remaining_or_next_caller() {
        for keep_waiter in [false, true] {
            let server = TokenServer::start(vec![Exchange::new(
                LOGIN,
                200,
                &token_body("access", Some("refresh"), 3600),
            )]);
            let auth = server.authentication();
            let mut first = auth.token().boxed();
            assert!(futures_util::poll!(&mut first).is_pending());
            let mut second = auth.token().boxed();
            if keep_waiter {
                assert!(futures_util::poll!(&mut second).is_pending());
            }
            drop(first);
            assert_eq!(second.await.unwrap().access, "access");
            server.finish();
        }
    }

    #[tokio::test]
    async fn late_token_waiters_cannot_undo_a_rejection() {
        let server = TokenServer::start(vec![
            Exchange::new(LOGIN, 200, &token_body("first", Some("refresh"), 3600)),
            Exchange::new(REFRESH, 200, &token_body("second", None, 3600)),
        ]);
        let auth = server.authentication();
        let mut first = auth.token().boxed();
        let mut late = auth.token().boxed();
        assert!(futures_util::poll!(&mut first).is_pending());
        assert!(futures_util::poll!(&mut late).is_pending());
        let rejected = first.await.unwrap();
        auth.reject(&rejected);
        assert!(Arc::ptr_eq(&rejected, &late.await.unwrap()));
        assert_eq!(auth.token().await.unwrap().access, "second");
        server.finish();
    }

    #[test]
    fn refresh_keeps_the_previous_token_only_when_the_replacement_is_absent() {
        for (previous, replacement, expected) in [
            (None, None, None),
            (None, Some(""), None),
            (None, Some("replacement"), Some("replacement")),
            (Some("previous"), None, Some("previous")),
            (Some("previous"), Some(""), None),
            (Some("previous"), Some("replacement"), Some("replacement")),
        ] {
            let reply = TokenReply {
                access_token: "access".into(),
                refresh_token: replacement.map(str::to_owned),
                token_type: "bEaReR".into(),
                expires_in: 3600,
            };
            let result = Token::from_reply(reply, previous, Instant::now());
            match expected {
                Some(expected) => assert_eq!(result.unwrap().refresh, expected),
                None => assert!(matches!(result, Err(Error::Protocol(_)))),
            }
        }
    }

    proptest! {
        #[test]
        fn refresh_is_scheduled_in_the_second_half_of_the_token_lifetime(
            seconds in 1..=u32::MAX,
        ) {
            let started = Instant::now();
            let reply = TokenReply {
                access_token: "access".into(), refresh_token: Some("refresh".into()),
                token_type: "Bearer".into(), expires_in: u64::from(seconds),
            };
            let token = Token::from_reply(reply, None, started).unwrap();
            let lifetime = Duration::from_secs(u64::from(seconds));
            let refresh_after = token.refresh_at.duration_since(started);
            prop_assert!(refresh_after >= lifetime / 2);
            prop_assert!(refresh_after <= lifetime);
        }
    }

    #[test]
    fn refresh_deadlines_respect_margin_and_half_lifetime_boundaries() {
        for (lifetime_seconds, expected_millis) in [
            (1, 500),
            (60, 30_000),
            (61, 30_500),
            (120, 60_000),
            (3600, 3_540_000),
        ] {
            let started = Instant::now();
            let reply = TokenReply {
                access_token: "access".into(),
                refresh_token: Some("refresh".into()),
                token_type: "Bearer".into(),
                expires_in: lifetime_seconds,
            };
            let token = Token::from_reply(reply, None, started).unwrap();
            assert_eq!(
                token.refresh_at.duration_since(started),
                Duration::from_millis(expected_millis),
                "lifetime {lifetime_seconds}s",
            );
        }
    }

    #[test]
    fn invalid_token_replies_are_rejected() {
        for (access, kind, lifetime) in [
            ("", "Bearer", 60),
            ("access", "Basic", 60),
            ("access", "Bearer", 0),
            ("access", "Bearer", u64::MAX),
        ] {
            let reply = TokenReply {
                access_token: access.into(),
                refresh_token: Some("refresh".into()),
                token_type: kind.into(),
                expires_in: lifetime,
            };
            assert!(matches!(
                Token::from_reply(reply, None, Instant::now()),
                Err(Error::Protocol(_))
            ));
        }
    }

    #[tokio::test]
    async fn stale_rejections_preserve_the_current_token() {
        let auth = Authentication::new(
            "login".into(),
            "password".into(),
            reqwest::Client::new(),
            Url::parse(protocol::TOKEN_URL).unwrap(),
        )
        .unwrap();
        let token = || {
            Arc::new(Token {
                access: "access".into(),
                refresh: "refresh".into(),
                refresh_at: Instant::now() + Duration::from_secs(60),
            })
        };
        let (old, current) = (token(), token());
        auth.state.lock().unwrap().current = Some(current.clone());

        auth.reject(&old);
        assert!(!auth.state.lock().unwrap().rejected);
        auth.reject(&current);
        assert!(auth.state.lock().unwrap().rejected);
    }
}
