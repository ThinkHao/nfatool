#!/bin/bash
mkdir -p /home/nfa95
useradd --system --home-dir /home/nfa95 --shell /sbin/nologin nfa95 || true
chown -R nfa95:nfa95 /home/nfa95