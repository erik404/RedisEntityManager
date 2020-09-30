<?php

namespace erik404\Annotation;

/**
 * @Annotation
 * @Target({"CLASS"})
 */
final class RedisDataType
{
    /** @Enum({"HASH", "LIST"}) */
    public string $type;
}