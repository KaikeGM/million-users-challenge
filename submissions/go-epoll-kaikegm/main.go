package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"syscall"

	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/sys/unix"
)

func envOr(name, fallback string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return fallback
}

func main() {
	// Ignore SIGPIPE to avoid dying on broken client connections
	signal.Ignore(syscall.SIGPIPE)

	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		log.Fatal("missing required environment variable DATABASE_URL")
	}
	secret := []byte(os.Getenv("JWT_SECRET"))
	if len(secret) == 0 {
		log.Fatal("missing required environment variable JWT_SECRET")
	}
	host := envOr("HOST", "127.0.0.1")
	port := envOr("PORT", "3000")
	extraPort := os.Getenv("EXTRA_PORT")

	procs := runtime.GOMAXPROCS(0)
	// 512 is the spec maximum. min(384, max(128, 24*procs)):
	// 16c -> 384, 8c -> 192, 4c -> 128.
	pool := min(384, max(128, 24*procs))
	if v := os.Getenv("DB_POOL"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			log.Fatalf("invalid DB_POOL %q", v)
		}
		pool = n
	}

	cfg, err := pgxpool.ParseConfig(dbURL)
	if err != nil {
		log.Fatalf("DATABASE_URL parse: %v", err)
	}
	cfg.MaxConns = int32(pool)
	if cfg.ConnConfig.RuntimeParams == nil {
		cfg.ConnConfig.RuntimeParams = make(map[string]string)
	}
	cfg.ConnConfig.RuntimeParams["plan_cache_mode"] = "force_generic_plan"
	cfg.ConnConfig.RuntimeParams["jit"] = "off"

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	db, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		log.Fatalf("db connect: %v", err)
	}
	defer db.Close()

	if err := db.Ping(ctx); err != nil {
		log.Fatalf("db ping: %v", err)
	}

	authChecker, err := NewAuthChecker(secret)
	if err != nil {
		log.Fatalf("auth verifier: %v", err)
	}

	dbWorkers := min(64, 4*procs)
	dbPool := NewDBPool(db, dbWorkers)

	// Create multi-reactor epoll workers across cores
	for i := 0; i < procs; i++ {
		epollFd, err := unix.EpollCreate1(unix.EPOLL_CLOEXEC)
		if err != nil {
			log.Fatalf("epoll_create1: %v", err)
		}

		l1, err := createListener(host, port)
		if err != nil {
			log.Fatalf("createListener port %s: %v", port, err)
		}
		ev1 := unix.EpollEvent{
			Events: unix.EPOLLIN | unix.EPOLLET,
			Fd:     int32(l1),
		}
		if err := unix.EpollCtl(epollFd, unix.EPOLL_CTL_ADD, l1, &ev1); err != nil {
			log.Fatalf("epoll_ctl l1: %v", err)
		}

		l2 := -1
		if extraPort != "" {
			var err error
			l2, err = createListener(host, extraPort)
			if err != nil {
				log.Fatalf("createListener extra port %s: %v", extraPort, err)
			}
			ev2 := unix.EpollEvent{
				Events: unix.EPOLLIN | unix.EPOLLET,
				Fd:     int32(l2),
			}
			if err := unix.EpollCtl(epollFd, unix.EPOLL_CTL_ADD, l2, &ev2); err != nil {
				log.Fatalf("epoll_ctl l2: %v", err)
			}
		}

		worker := &Worker{
			id:          i,
			epollFd:     epollFd,
			listener1:   l1,
			listener2:   l2,
			dbPool:      dbPool,
			authChecker: authChecker,
		}
		go worker.run()
	}

	log.Printf("go-epoll-kaikegm running on %s:%s (extra %s) with %d epoll workers, %d DB workers", host, port, extraPort, procs, dbWorkers)

	<-ctx.Done()
	log.Printf("Shutting down...")
}
