(defpackage #:rb-tree-asd
  (:use :cl :asdf))

(in-package :rb-tree-asd)

(asdf:defsystem rb-tree
  :name
  "rb-tree"
  :version
  "0.1"
  :components
  ((:file "package-rb-tree") 
   (:file "base")
   (:file "memory" :depends-on ("package-rb-tree" "base"))
   (:file "persistent" :depends-on ("package-rb-tree" "base"))
   (:file "text" :depends-on ("persistent"))
   )
  :depends-on
  ( ))

(asdf:defsystem rb-tree-tests
  :name
  "rb-tree-tests"
  :version
  "0.1"
  :components
  ((:file "tests"))
  :depends-on
  ("rb-tree" "lisp-unit"))


