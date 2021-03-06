<?php

namespace Oneup\UploaderBundle\Uploader\Storage;

use League\Flysystem\File;
use League\Flysystem\Plugin\ListFiles;
use Oneup\UploaderBundle\Uploader\Chunk\Storage\FlysystemStorage as ChunkStorage;
use Oneup\UploaderBundle\Uploader\File\FileInterface;
use Oneup\UploaderBundle\Uploader\File\FlysystemFile;
use Symfony\Component\HttpFoundation\Session\SessionInterface;

class FlysystemOrphanageStorage extends FlysystemStorage implements OrphanageStorageInterface
{
    protected $storage;
    protected $session;
    protected $chunkStorage;
    protected $config;
    protected $type;

    /**
     * @param StorageInterface      $storage
     * @param SessionInterface      $session
     * @param ChunkStorage          $chunkStorage This class is only used if the gaufrette chunk storage is used.
     * @param                       $config
     * @param                       $type
     */
    public function __construct(StorageInterface $storage, SessionInterface $session, ChunkStorage $chunkStorage, $config, $type)
    {
        /*
         * initiate the storage on the chunk storage's filesystem
         * the stream wrapper is useful for metadata.
         */
        parent::__construct($chunkStorage->getFilesystem(), $chunkStorage->bufferSize, $chunkStorage->getStreamWrapperPrefix());

        $this->storage = $storage;
        $this->chunkStorage = $chunkStorage;
        $this->session = $session;
        $this->config = $config;
        $this->type = $type;
    }

    public function upload(FileInterface $file, $name, $path = null)
    {
        if (!$this->session->isStarted())
            throw new \RuntimeException('You need a running session in order to run the Orphanage.');

        return parent::upload($file, $name, $this->getPath());
    }

    public function uploadFiles(array $files = null)
    {
        try {
            if (null === $files) {
                $files = $this->getFiles();
            }
            $return = array();

            foreach ($files as $key => $file) {
                try {
                    $return[] = $this->storage->upload($file, str_replace($this->getPath(), '', $key));
                } catch (\Exception $e) {
                    // well, we tried.
                    continue;
                }
            }

            return $return;
        } catch (\Exception $e) {
            return array();
        }
    }

    /**
     * Modified by GrizzlyLab
     * @return array
     */
    public function getFiles()
    {
        // add plugin to make it work
        $this->chunkStorage->getFilesystem()->addPlugin(new ListFiles());

        // files from filesystem
        $files = $this->chunkStorage->getFilesystem()->listFiles($this->getPath());
        $keys = [];
        foreach ($files as $file) {
            $keys[] = $file['path'];
        }

        // create some FlysystemFile
        $files = array();
        foreach ($keys as $key) {
            // gotta pass the filesystem to both as you can't get it out from one..
            $files[$key] = new FlysystemFile(new File($this->chunkStorage->getFilesystem(), $key), $this->chunkStorage->getFilesystem());
        }

        return $files;
    }

    protected function getPath()
    {
        // the storage is initiated in the root of the filesystem, from where the orphanage directory
        // should be relative.
        return sprintf('%s/%s/%s', $this->config['directory'], $this->session->getId(), $this->type);
    }

}
