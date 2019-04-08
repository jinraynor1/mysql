<?php

namespace React\MySQL\Io;

use Evenement\EventEmitter;
use React\MySQL\Commands\QueryCommand;
use React\Socket\ConnectionInterface;
use React\Stream\ReadableStreamInterface;
use React\Stream\Util;
use React\Stream\WritableStreamInterface;

/**
 * @internal
 * @see Connection::queryStream()
 */
class QueryStream extends EventEmitter implements ReadableStreamInterface
{
    private $query;
    private $connection;
    private $started = false;
    private $closed = false;
    private $paused = false;

    public function __construct(QueryCommand $command, ConnectionInterface $connection)
    {
        $this->command = $command;
        $this->connection = $connection;

        // forward result set rows until result set end
        $self = $this;
        $command->on('result', function ($row) use($self) {
            if (!$self->started && $self ->paused) {
                $self ->connection->pause();
            }
            $self ->started = true;

            $self ->emit('data', array($row));
        });
        $command->on('end', function () use($self) {
            $self->emit('end');
            $self->close();
        });

        // status reply (response without result set) ends stream without data
        $command->on('success', function () use($self) {
            $self->emit('end');
            $self->close();
        });
        $command->on('error', function ($err) use($self) {
            $self->emit('error', array($err));
            $self->close();
        });
    }

    public function isReadable()
    {
        return !$this->closed;
    }

    public function pause()
    {
        $this->paused = true;
        if ($this->started && !$this->closed) {
            $this->connection->pause();
        }
    }

    public function resume()
    {
        $this->paused = false;
        if ($this->started && !$this->closed) {
            $this->connection->resume();
        }
    }

    public function close()
    {
        if ($this->closed) {
            return;
        }

        $this->closed = true;
        if ($this->started && $this->paused) {
            $this->connection->resume();
        }

        $this->emit('close');
        $this->removeAllListeners();
    }

    public function pipe(WritableStreamInterface $dest, array $options = array())
    {
        return Util::pipe($this, $dest, $options);
    }
}
