# The whole lab in one image: three .NET services, two Python simulators and NATS.
# Built remotely by the hosting platform; nothing here needs Docker on the laptop.

FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src
COPY LinePulse.sln ./
COPY src/ src/
RUN dotnet publish src/LinePulse.Core/LinePulse.Core.csproj -c Release -o /out/core --nologo \
 && dotnet publish src/LinePulse.Collector/LinePulse.Collector.csproj -c Release -o /out/collector --nologo \
 && dotnet publish src/LinePulse.Broker/LinePulse.Broker.csproj -c Release -o /out/broker --nologo

# The NATS server binary, taken from the official image rather than downloaded by hand.
# It is a static Go binary, so it runs unchanged on the Debian runtime below.
FROM nats:2.10-alpine AS nats

FROM mcr.microsoft.com/dotnet/aspnet:8.0
# tzdata: the shift calendar resolves Europe/Istanbul and Europe/Belgrade and refuses to
# start if it cannot, which is deliberate, so the zone database must be present.
RUN apt-get update \
 && apt-get install -y --no-install-recommends python3 python3-venv tzdata netcat-openbsd curl \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt ./
RUN python3 -m venv /app/.venv && /app/.venv/bin/pip install --no-cache-dir -r requirements.txt

COPY --from=nats /usr/local/bin/nats-server /usr/local/bin/nats-server
COPY --from=build /out/ /app/
COPY sim/ /app/sim/
COPY deploy/start.sh /app/start.sh
RUN chmod +x /app/start.sh && mkdir -p /app/data

ENV DOTNET_CLI_TELEMETRY_OPTOUT=1 \
    LP_SPEED=20 \
    Ingest__TimeScale=20 \
    PORT=8080
EXPOSE 8080
CMD ["/app/start.sh"]
