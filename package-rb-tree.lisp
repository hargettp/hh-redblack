(defpackage #:rb-tree-asd
  (:use :cl :asdf))

(in-package :rb-tree-asd)

(defpackage :rb-tree
  (:use :cl)
  (:export

   #:make-red-black-tree   
   #:make-memory-persistent-red-black-tree
   #:make-text-file-red-black-tree

   #:rb-put
   #:rb-get
   #:rb-remove
   #:rb-size
   #:rb-keys
   #:with-rb-keys-and-data
   #:with-rb-transaction

   #:rb-key-compare

   #:requires-red-black-transaction

   ))