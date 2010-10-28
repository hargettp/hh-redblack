(defpackage #:rb-tree-asd
  (:use :cl :asdf))

(in-package :rb-tree-asd)

(defpackage :rb-tree
  (:use :cl)
  (:export

   #:make-red-black-tree
   
   #:open-persistent-red-black-tree

   #:rb-put
   #:rb-get
   #:rb-remove
   #:rb-size
   #:with-rb-keys-and-data

   #:rb-key-compare

   ))