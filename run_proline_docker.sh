#!/bin/sh
docker run --rm -v "$(pwd)/config":"/proline/config" -v "$(pwd)/proline_results":"/proline/proline_results" proline-cli:19.11.06