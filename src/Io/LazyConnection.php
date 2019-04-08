<?php

namespace React\MySQL\Io;

use React\MySQL\ConnectionInterface;
use Evenement\EventEmitter;
use React\MySQL\Exception;
use React\MySQL\Factory;
use React\EventLoop\LoopInterface;
use React\MySQL\QueryResult;

/**
 * @internal
 * @see \React\MySQL\Factory::createLazyConnection()
 */
class LazyConnection extends EventEmitter implements ConnectionInterface
{
    private $factory;
    private $uri;
    public $connecting;
    private $closed = false;
    private $busy = false;

    /**
     * @var ConnectionInterface|null
     */
    private $disconnecting;

    private $loop;
    private $idlePeriod = 60.0;
    public $idleTimer;
    private $pending = 0;

    public function __construct(Factory $factory, $uri, LoopInterface $loop)
    {
        $args = array();
        \parse_str(\parse_url($uri, \PHP_URL_QUERY), $args);
        if (isset($args['idle'])) {
            $this->idlePeriod = (float)$args['idle'];
        }

        $this->factory = $factory;
        $this->uri = $uri;
        $this->loop = $loop;
    }

    private function connecting()
    {
        if ($this->connecting !== null) {
            return $this->connecting;
        }

        // force-close connection if still waiting for previous disconnection
        if ($this->disconnecting !== null) {
            $this->disconnecting->close();
            $this->disconnecting = null;
        }
        $self = $this;
        $this->connecting = $connecting = $this->factory->createConnection($this->uri);
        $this->connecting->then(function (ConnectionInterface $connection) use($self) {
            // connection completed => remember only until closed
            $connection->on('close', function () use($self) {
                $self->connecting = null;

                if ($self->idleTimer !== null) {
                    $self->loop->cancelTimer($self->idleTimer);
                    $self->idleTimer = null;
                }
            });
        }, function () use($self) {
            // connection failed => discard connection attempt
            $self->connecting = null;
        });

        return $connecting;
    }

    public function awake()
    {
        ++$this->pending;

        if ($this->idleTimer !== null) {
            $this->loop->cancelTimer($this->idleTimer);
            $this->idleTimer = null;
        }
    }

    public function idle()
    {
        --$this->pending;
        $self = $this;
        if ($this->pending < 1 && $this->idlePeriod >= 0) {
            $this->idleTimer = $this->loop->addTimer($this->idlePeriod, function () use($self) {
                $self->connecting->then(function (ConnectionInterface $connection) use($self) {
                    $self->disconnecting = $connection;
                    $connection->quit()->then(
                        function () use($self) {
                            // successfully disconnected => remove reference
                            $self->disconnecting = null;
                        },
                        function () use ($connection, $self) {
                            // soft-close failed => force-close connection
                            $connection->close();
                            $self->disconnecting = null;
                        }
                    );
                });
                $self->connecting = null;
                $self->idleTimer = null;
            });
        }
    }

    public function query($sql, array $params = array())
    {
        if ($this->closed) {
            return \React\Promise\reject(new Exception('Connection closed'));
        }
        $self = $this;
        return $this->connecting()->then(function (ConnectionInterface $connection) use ($sql, $params, $self) {
            $self->awake();
            return $connection->query($sql, $params)->then(
                function (QueryResult $result) use($self) {
                    $self->idle();
                    return $result;
                },
                function (\Exception $e) use($self) {
                    $self->idle();
                    throw $e;
                }
            );
        });
    }

    public function queryStream($sql, $params = array())
    {
        if ($this->closed) {
            throw new Exception('Connection closed');
        }
        $self = $this;
        return \React\Promise\Stream\unwrapReadable(
            $this->connecting()->then(function (ConnectionInterface $connection) use ($sql, $params, $self) {
                $stream = $connection->queryStream($sql, $params);

                $self->awake();
                $stream->on('close', function () use($self) {
                    $self->idle();
                });

                return $stream;
            })
        );
    }

    public function ping()
    {
        if ($this->closed) {
            return \React\Promise\reject(new Exception('Connection closed'));
        }
        $self = $this;
        return $this->connecting()->then(function (ConnectionInterface $connection) use($self) {
            $self->awake();
            return $connection->ping()->then(
                function () use($self) {
                    $self->idle();
                },
                function (\Exception $e) use($self){
                    $self->idle();
                    throw $e;
                }
            );
        });
    }

    public function quit()
    {
        if ($this->closed) {
            return \React\Promise\reject(new Exception('Connection closed'));
        }

        // not already connecting => no need to connect, simply close virtual connection
        if ($this->connecting === null) {
            $this->close();
            return \React\Promise\resolve();
        }
        $self = $this;
        return $this->connecting()->then(function (ConnectionInterface $connection) use($self) {
            $self->awake();
            return $connection->quit()->then(
                function () use($self) {
                    $self->close();
                },
                function (\Exception $e) use($self) {
                    $self->close();
                    throw $e;
                }
            );
        });
    }

    public function close()
    {
        if ($this->closed) {
            return;
        }

        $this->closed = true;

        // force-close connection if still waiting for previous disconnection
        if ($this->disconnecting !== null) {
            $this->disconnecting->close();
            $this->disconnecting = null;
        }

        // either close active connection or cancel pending connection attempt
        if ($this->connecting !== null) {
            $this->connecting->then(function (ConnectionInterface $connection) {
                $connection->close();
            });
            $this->connecting->cancel();
            $this->connecting = null;
        }

        if ($this->idleTimer !== null) {
            $this->loop->cancelTimer($this->idleTimer);
            $this->idleTimer = null;
        }

        $this->emit('close');
        $this->removeAllListeners();
    }
}
