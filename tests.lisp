;; Copyright (c) 2011 Phil Hargett

;; Permission is hereby granted, free of charge, to any person obtaining a copy
;; of this software and associated documentation files (the "Software"), to deal
;; in the Software without restriction, including without limitation the rights
;; to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
;; copies of the Software, and to permit persons to whom the Software is
;; furnished to do so, subject to the following conditions:

;; The above copyright notice and this permission notice shall be included in
;; all copies or substantial portions of the Software.

;; THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
;; IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
;; FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
;; AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
;; LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
;; OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
;; THE SOFTWARE.

(defpackage :hh-redblack-tests
  (:use :cl :lisp-unit :hh-redblack))

(in-package :hh-redblack-tests)

(remove-all-tests)

(define-test create-rb-rtree-tests
    (let ((tree (make-red-black-tree)))
      (assert-true tree)
      (assert-eq :black (hh-redblack::color (hh-redblack::root tree)))
      (assert-false (hh-redblack::rb-first tree))
      (assert-false (hh-redblack::rb-last tree))))

(define-test create-rb-node-tests
    (let ((node (make-instance 'hh-redblack::memory-red-black-node)))
      (assert-true node)))

(define-test put-tests
  (let ((tree (make-red-black-tree)))
      (rb-put tree 1 "one")
      (assert-eq :black (hh-redblack::color (hh-redblack::root tree)))
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

(define-test remove-tests
  (assert-equal `(1 2 4 5) 
		(let ((tree (make-red-black-tree))
		      (keys ()))
		  (rb-put tree 4 "four")
		  (rb-put tree 1 "one")
		  (rb-put tree 5 "five")
		  (rb-put tree 3 "three")
		  (rb-put tree 2 "two")
		  (rb-remove tree 3)
		  (with-rb-keys-and-data (key data :first) tree
					 (setf keys (append keys (list key))))
		  keys))
  
  (let ((tree (make-red-black-tree)))
    (rb-put tree 4 "four")
    (rb-put tree 1 "one")
    (rb-put tree 5 "five")
    (rb-put tree 3 "three")
    (rb-put tree 2 "two")
    (assert-equal `(1 2 3 4 5) 
		  (let ((keys ()))
		    (with-rb-keys-and-data (key data :first) tree
					   (setf keys (append keys (list key))))
		    keys))
    (rb-remove tree 3)
    (rb-remove tree 4)
    (assert-equal `(1 2 5) 
		  (let ((keys ()))
    		    (with-rb-keys-and-data (key data :first) tree
    					   (setf keys (append keys (list key))))
    		    keys))
    (rb-put tree 3 "three")
    (rb-put tree 4 "four")
    (assert-equal `(1 2 3 4 5) 
		  (let ((keys ()))
    		    (with-rb-keys-and-data (key data :first) tree
    					   (setf keys (append keys (list key))))
    		    keys))))

(defmacro with-temporary-tree ((var) &rest body)
  `(let ((temp-file-name (asdf:system-relative-pathname (asdf:find-system "hh-redblack") 
							(format nil "text-~s.tree" (random (expt 2 32))))))
     (unwind-protect 
	  (let ((,var (make-text-file-red-black-tree temp-file-name)))
	    ,@body)
	  (delete-file temp-file-name))))

(define-test peristent-red-black-tree-tests
  (with-temporary-tree (tree)
    (assert-true tree))

  (with-temporary-tree (tree)
    (assert-error 'requires-red-black-transaction
    		  (rb-put tree 1 "one")))

  (with-temporary-tree (tree)
    (with-rb-transaction (tree)
      (rb-put tree 1 "one"))
    (assert-error 'requires-red-black-transaction
    		  (rb-get tree 1)))

  (with-temporary-tree (tree)
    (with-rb-transaction (tree)
      (rb-put tree 1 "one"))
    (with-rb-transaction (tree)
      (rb-put tree 2 "two"))
    (with-rb-transaction (tree)
      (assert-equal `(1 2) (rb-keys tree))))

  (with-temporary-tree (tree)
    (with-rb-transaction (tree)
      (rb-put tree 1 "one"))
    (with-rb-transaction (tree)
      (rb-put tree 2 "two"))
    (with-rb-transaction (tree)
      (rb-put tree 3 "two"))
    (with-rb-transaction (tree)
      (rb-remove tree 2))
    (with-rb-transaction (tree)
      (assert-equal `(1 3) (rb-keys tree))))

  (with-temporary-tree (tree)
    (with-rb-transaction (tree)
      (rb-put tree 4 "four")
      (rb-put tree 1 "one")
      (rb-put tree 5 "five")
      (rb-put tree 3 "three")
      (rb-put tree 2 "two")
      (assert-equal `(1 2 3 4 5) 
		    (let ((keys ()))
		      (with-rb-keys-and-data (key data :first) tree
					     (setf keys (append keys (list key))))
		      keys))))

(with-temporary-tree (tree)
  (with-rb-transaction (tree)
    (rb-put tree 4 "four")
    (rb-put tree 1 "one")
    (rb-put tree 5 "five")
    (rb-put tree 3 "three")
    (rb-put tree 2 "two")
    (assert-equal `(1 2 3 4 5) 
		  (let ((keys ()))
		    (with-rb-keys-and-data (key data :first) tree
					   (setf keys (append keys (list key))))
		    keys)))

  (with-rb-transaction (tree)
    (rb-remove tree 3)
    (assert-equal `(1 2 4 5) 
		  (let ((keys ()))
    		    (with-rb-keys-and-data (key data :first) tree
    					   (setf keys (append keys (list key))))
    		    keys)))

  (with-rb-transaction (tree)
    (rb-remove tree 4)
    (assert-equal `(1 2 5) 
		  (let ((keys ()))
    		    (with-rb-keys-and-data (key data :first) tree
    					   (setf keys (append keys (list key))))
    		    keys)))

  (assert-equal  `("one" "two" "three" "four" "five")
		 (with-temporary-tree (tree)
		   (with-rb-transaction (tree)
		     (let ((all-data ()))
		       (rb-put tree 4 "four")
		       (rb-put tree 1 "one")
		       (rb-put tree 5 "five")
		       (rb-put tree 3 "three")
		       (rb-put tree 2 "two")
		       (with-rb-keys-and-data (key data :first) tree
					      (setf all-data (append all-data (list (hh-redblack::contents data)))))
		       all-data))))

  (assert-equal  `("one" "two" "three" "four" "five")
		 (with-temporary-tree (tree)
		   (with-rb-transaction (tree)
		     (rb-put tree 4 "four")
		     (rb-put tree 1 "one")
		     (rb-put tree 5 "five")
		     (rb-put tree 3 "three")
		     (rb-put tree 2 "two")
		     (loop for key in (rb-keys tree)
			  collect (rb-get tree key)))))

  (let ((temp-file-name (asdf:system-relative-pathname (asdf:find-system "hh-redblack") 
						       (format nil "text-~s.tree" (random (expt 2 32))))))
    (unwind-protect 
	 (progn
	   (let ((tree (make-text-file-red-black-tree temp-file-name)))
	     (with-rb-transaction (tree)
	       (rb-put tree 1 "one"))
	     (with-rb-transaction (tree)
	       (assert-equal `(1) (rb-keys tree))))

	   (let ((tree (make-text-file-red-black-tree temp-file-name)))
	     (with-rb-transaction (tree)
	       (rb-put tree 2 "two"))
	     (with-rb-transaction (tree)
	       (assert-equal `(1 2) (rb-keys tree))))

	   (let ((tree (make-text-file-red-black-tree temp-file-name)))
	     (with-rb-transaction (tree)
	       (assert-equal `(1 2) (rb-keys tree)))))
      (delete-file temp-file-name)))

  (with-rb-transaction (tree)
    (rb-put tree 3 "three")
    (rb-put tree 4 "four")
    (assert-equal `(1 2 3 4 5) 
		  (let ((keys ()))
    		    (with-rb-keys-and-data (key data :first) tree
    					   (setf keys (append keys (list key))))
    		    keys)))))

(define-test larger-tree-tests
    (let ((tree (make-red-black-tree)))
      (rb-put tree 5 "five")
      (rb-put tree 3 "three")
      (rb-put tree 6 "six")
      (rb-put tree 1 "one")
      (rb-put tree 8 "eight")
      (rb-put tree 2 "two")
      (rb-put tree 9 "nine")
      (rb-put tree 7 "seven")
      (rb-put tree 10 "ten")
      (rb-put tree 4 "four")
      (rb-remove tree 3)
      (assert-equal `(1 2 4 5 6 7 8 9 10) 
		    (rb-keys tree)))

  (let ((tree (make-memory-persistent-red-black-tree)))
    (with-rb-transaction (tree)
      (rb-put tree 5 "five")
      (rb-put tree 3 "three")
      (rb-put tree 6 "six")
      (rb-put tree 1 "one")
      (rb-put tree 8 "eight")
      (rb-put tree 2 "two")
      (rb-put tree 9 "nine")
      (rb-put tree 7 "seven")
      (rb-put tree 10 "ten")
      (rb-put tree 4 "four"))
    (with-rb-transaction (tree)
      (rb-remove tree 3))
      (assert-equal `(1 2 4 5 6 7 8 9 10)
		    (with-rb-transaction (tree)
		      (rb-keys tree))))

  (with-temporary-tree (tree)
    (with-rb-transaction (tree)
      (rb-put tree 5 "five")
      (rb-put tree 3 "three")
      (rb-put tree 6 "six")
      (rb-put tree 1 "one")
      (rb-put tree 8 "eight")
      (rb-put tree 2 "two")
      (rb-put tree 9 "nine")
      (rb-put tree 7 "seven")
      (rb-put tree 10 "ten")
      (rb-put tree 4 "four"))
    (with-rb-transaction (tree)
      (rb-remove tree 3))
    (assert-equal `(1 2 4 5 6 7 8 9 10)
		  (with-rb-transaction (tree)
		    (rb-keys tree)))))

(run-tests)