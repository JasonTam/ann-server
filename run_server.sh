#!/usr/bin/env bash

gunicorn "api.app_falcon:build_app()"