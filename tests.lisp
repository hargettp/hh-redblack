(defpackage :rb-tree-tests
  (:use :cl :lisp-unit :rb-tree))

(in-package :rb-tree-tests)

(remove-all-tests)

(define-test create-rb-rtree-tests
    (let ((tree (make-red-black-tree)))
      (assert-true tree)
      (assert-eq :black (rb-tree::color (rb-tree::root tree)))
      (assert-false (rb-tree::rb-first tree))
      (assert-false (rb-tree::rb-last tree))))

(define-test create-rb-node-tests
    (let ((node (make-instance 'rb-tree::memory-red-black-node)))
      (assert-true node)))

(define-test put-tests
  (let ((tree (make-red-black-tree)))
      (rb-put tree 1 "one")
      (assert-eq :black (rb-tree::color (rb-tree::root tree)))
      (assert-true t)))

(define-test put-get-tests
  (let ((tree (make-red-black-tree)))
      (rb-put tree 1 "one")
      (assert-true (string= "one" (rb-get tree 1)))
      (rb-put tree 2 "two")
      (assert-true (string= "one" (rb-get tree 1)))
      (assert-true (string= "two" (rb-get tree 2)))))

(define-test iteration-tests
  (assert-equal `(1 2 3 4 5) 
		(let ((tree (make-red-black-tree))
		       (keys ()))
		   (rb-put tree 4 "four")
		   (rb-put tree 1 "one")
		   (rb-put tree 5 "five")
		   (rb-put tree 3 "three")
		   (rb-put tree 2 "two")
		   (with-rb-keys-and-data (key data :first) tree
					  (setf keys (append keys (list key))))
		   keys))
  (assert-equal `(5 4 3 2 1) 
		(let ((tree (make-red-black-tree))
		       (keys ()))
		   (rb-put tree 4 "four")
		   (rb-put tree 1 "one")
		   (rb-put tree 5 "five")
		   (rb-put tree 3 "three")
		   (rb-put tree 2 "two")
		   (with-rb-keys-and-data (key data :last) tree
					  (setf keys (append keys (list key))))
		   keys))
  (assert-equal  `("one" "two" "three" "four" "five") 
		 (let ((tree (make-red-black-tree))
		       (all-data ()))
		   (rb-put tree 4 "four")
		   (rb-put tree 1 "one")
		   (rb-put tree 5 "five")
		   (rb-put tree 3 "three")
		   (rb-put tree 2 "two")
		   (with-rb-keys-and-data (key data :first) tree
					  (setf all-data (append all-data (list data))))
		   all-data)))

(run-tests)