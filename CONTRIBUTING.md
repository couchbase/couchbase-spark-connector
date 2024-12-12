# Contributing

Welcome, and thanks for lending a hand! Before we get started, please read the
[Couchbase Code of Conduct](CODE_OF_CONDUCT.md).

## Discussion

We'd love for you to join us on the [Couchbase Forum](https://forums.couchbase.com) or [Discord](https://discord.com/invite/sQ5qbPZuTh).

## Reporting Issues

The simplest way to help is to report bugs or request new features, which you can on [GitHub](https://github.com/couchbase/couchbase-spark-connector/).

## Pull Requests

We use Gerrit for code review. The GitHub repo is a read-only mirror of the version in Gerrit.

1. Visit [https://review.couchbase.org](https://review.couchbase.org) and register for an account.
2. Agree to the CLA by visiting [https://review.couchbase.org/settings/#Agreements](https://review.couchbase.org/settings/#Agreements)
   Otherwise, you won't be able to push changes to Gerrit (instead getting a "`Upload denied for project`" message).
3. Clone the project `git clone https://github.com/couchbase/couchbase-spark-connector/ && cd couchbase-spark-connector`.
4. Setup Gerrit remote `git remote add gerrit ssh://YOUR_SSH_USERNAME@review.couchbase.org:29418/couchbase-java-client.git`
5. Setup Gerrit commit hooks `cd .git/hooks && wget http://review.couchbase.com/tools/hooks/commit-msg && chmod +x commit-msg`
6. Make your change and `git commit`.
7. Push your change to Gerrit with `git push gerrit HEAD:refs/for/master`.
