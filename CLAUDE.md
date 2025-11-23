# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Scrape Forexfactory.com on demand, returning a DataFrame of `Event`s.

Used as an editable package.

## Architecture

Modular, loosely coupled.

## Code Conventions

- Follow Pythonic best practices, e.g. all functions need docstrings and type hints
- Do not use `print` when `logging` should be used.
- Currencies use `Currency` not strings

## Versioning

* Never work on `main`, only on feature/bug branches
* Commit your work with a descriptive message.
