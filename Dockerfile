# Build stage
FROM golang:1.25-alpine AS builder

# Set working directory
WORKDIR /app

# Copy go.mod and go.sum files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy the source code
COPY . .

# Build the application
RUN env GOOS=linux go build -o psproxy .

# Runtime stage
FROM alpine:latest

# Install CA certificates for HTTPS connections
RUN apk --no-cache add ca-certificates

WORKDIR /app

# Copy the binary from the builder stage
COPY --from=builder /app/psproxy /app/
# Copy the default config file
COPY --from=builder /app/shadow-proxy.yaml /app/

# Expose the proxy port (default 8090 from config)
EXPOSE 8090

# Run the application
ENTRYPOINT ["/app/psproxy"]
# Use the default config file or override with -c flag
CMD ["--config", "shadow-proxy.yaml"]