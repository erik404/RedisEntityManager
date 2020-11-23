<?php

namespace erik404;

use Doctrine\Common\Annotations\AnnotationReader;
use erik404\Annotation\RedisDataType;
use Exception;
use Predis\Client;
use ReflectionClass;
use ReflectionException;
use ReflectionObject;

class RedisEntityManager
{
    const HASH = 'HASH';
    const LIST = 'LIST';

    private Client $client;
    private AnnotationReader $reader;

    public function __construct(string $host, string $port, string $password)
    {
        $this->reader = new AnnotationReader();
        $this->client = new Client([
            'scheme' => 'tcp',
            'host' => $host,
            'port' => $port,
            'password' => $password
        ]);
        $this->client->connect();
    }

    /**
     * @param object $entity
     * @throws ReflectionException
     */
    public function persist(object $entity): void
    {
        $reflClass = new ReflectionClass(get_class($entity));
        $classAnnotations = $this->reader->getClassAnnotations($reflClass);
        foreach ($classAnnotations as $annotation) {
            if ($annotation instanceof RedisDataType) {
                $this->storeDataType($annotation->type, $entity);
                break;
            }
        }
    }

    /**
     * @param string $type
     * @param object $entity
     * @throws Exception
     */
    private function storeDataType(string $type, object $entity): void
    {
        switch ($type) {
            case self::HASH:
                $this->storeHash($entity);
                break;
            case self::LIST:
                $this->storeList($entity);
                break;
            default:
                throw new Exception(sprintf("UNKNOWN DATATYPE (%s)", $type));
        }
    }

    /**
     * @param object $entity
     * @throws ReflectionException
     */
    private function storeHash(object $entity): void
    {
        $hashName = $this->getIdentifierFromEntity(get_class($entity), self::HASH);
        $reflObject = new ReflectionObject($entity);
        foreach ($reflObject->getProperties() as $property) {
            $prop = $reflObject->getProperty($property->getName());
            $prop->setAccessible(true);
            $this->client->hset($hashName, $property->getName(), serialize($prop->getValue($entity)));
        }
    }

    /**
     * @param string $entity
     * @param string $type
     * @return string
     * @throws Exception
     */
    private function getIdentifierFromEntity(string $entity, string $type): string
    {
        switch ($type) {
            case self::HASH:
                $prefix = 'H_';
                break;
            case self::LIST:
                $prefix = 'L_';
                break;
            default:
                throw new Exception(sprintf("UNKNOWN DATATYPE (%s)", $type));
        }

        return strtoupper($prefix . str_replace('\\', '_', $entity));
    }

    /**
     * @param object $entity
     * @throws Exception
     */
    private function storeList(object $entity): void
    {
        $listName = $this->getIdentifierFromEntity(get_class($entity), self::LIST);
        $this->client->rpush($listName, serialize($entity));
    }

    /**
     * @param string $entity
     * @param int $amount
     * @return object|array
     * @throws ReflectionException
     */
    public function fetch(string $entity, int $amount = 1)
    {
        $reflClass = new ReflectionClass($entity);
        $classAnnotations = $this->reader->getClassAnnotations($reflClass);
        try {
            foreach ($classAnnotations as $annotation) {
                if ($annotation instanceof RedisDataType) {
                    return $this->fetchDataType($annotation->type, $entity, $amount);
                }
            }
        } catch (Exception $e) {
            // return null for now
        }

        return null;
    }

    /**
     * @param string $type
     * @param string $entity
     * @param int $amount
     * @return object|array
     * @throws ReflectionException
     */
    private function fetchDataType(string $type, string $entity, int $amount)
    {
        switch ($type) {
            case self::HASH:
                return $this->fetchHash($entity);
            case self::LIST:
                return $this->fetchList($entity, $amount);
            default:
                throw new Exception(sprintf("UNKNOWN DATATYPE (%s)", $type));
        }
    }

    /**
     * @param string $entity
     * @return object
     * @throws ReflectionException
     */
    private function fetchHash(string $entity): ?object
    {
        $entity = new $entity;
        $hashName = $this->getIdentifierFromEntity(get_class($entity), self::HASH);
        $reflObject = new ReflectionObject($entity);

        try {
            foreach ($reflObject->getProperties() as $property) {
                $prop = $reflObject->getProperty($property->getName());
                $prop->setAccessible(true);
                $prop->setValue($entity, unserialize($this->client->hget($hashName, $property->getName())));
            }
        } catch (Exception $e) {
            return null;
        }

        return $entity;
    }

    /**
     * @param string $entity
     * @param int $amount
     * @return array
     * @throws Exception
     */
    private function fetchList(string $entity, int $amount): array
    {
        $listName = $this->getIdentifierFromEntity($entity, self::LIST);
        $result = [];
        foreach ($this->client->lrange($listName, -$amount, -1) as $listItem) {
            $result[] = unserialize($listItem);
        }

        return $result;
    }

}
