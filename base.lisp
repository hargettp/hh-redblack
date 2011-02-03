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

(in-package :hh-redblack)

;; =====================================================================================================================
;;
;; Common definitions
;;
;; ---------------------------------------------------------------------------------------------------------------------

;; ---------------------------------------------------------------------------------------------------------------------
;; Interfaces
;; ---------------------------------------------------------------------------------------------------------------------

;; Nodes

(defclass red-black-node () ())

(macrolet 
    ((define-slot (name)
       `(progn
	  (defgeneric ,name (node))
	  (defgeneric (setf ,name) (value node)))))
  (define-slot parent)
  (define-slot left)
  (define-slot right)
  (define-slot color)
  (define-slot key)
  (define-slot data))

;; Trees

(defclass red-black-tree () ())

(defgeneric root (tree))
(defgeneric (setf root) (value tree))

(defgeneric leafp (tree node))

;; ---------------------------------------------------------------------------------------------------------------------
;; types
;; ---------------------------------------------------------------------------------------------------------------------

;; ---------------------------------------------------------------------------------------------------------------------
;; generics
;; ---------------------------------------------------------------------------------------------------------------------

(defgeneric rb-node-class (tree)
  (:documentation "Return the class to be used for creating nodes in the tree"))

(defgeneric rb-make-node (tree &key key data)
  (:documentation "Return a node suitable for insertion within the tree"))

(defgeneric rb-insert (tree node))

(defgeneric rb-insert-fixup (tree node))

(defgeneric rb-left-rotate (tree node))

(defgeneric rb-right-rotate (tree node))

(defgeneric rb-delete (tree node))

(defgeneric rb-transplant (tree u v))

(defgeneric rb-delete-fixup (tree node))

(defgeneric rb-tree-minimum (tree node))

(defgeneric rb-tree-maximum (tree node))

(defgeneric rb-find (tree key))

(defgeneric rb< (left-key right-key)
  (:documentation "Return t if the left key is less than the right key, nil otherwise"))

(defgeneric rb= (left-key right-key)
  (:documentation "Return t if the left key and right key are equivalent"))

(defgeneric rb-key-compare (left-key right-key)
  (:documentation "Compare 2 keys used in a red-black tree, return :less, :equal, or :greater, depending
    on the result of the comparison.  Definitions exist for common types such
    as numbers, strings, and symbols."))

(defgeneric rb-put (tree key data)
  (:documentation "Equivalent to sethash with a hashtable: set the data for a given key in the provided tree"))

(defgeneric rb-get (tree key &optional default)
  (:documentation "Returns 2 values, just like gethash: if the key is present, returns the 
   associated data and t (indicating data was present), otherwise returns 
   the default and nil"))

(defgeneric rb-remove (tree key)
  (:documentation "Remove the node with the indicated key from the tree"))

(defgeneric rb-size (tree)
  (:documentation "Calculate the number of nodes in the tree"))

(defgeneric rb-first (tree))

(defgeneric rb-last (tree))

(defgeneric rb-next (tree node)
  (:documentation "Return the node with the next higher key in the tree"))

(defgeneric rb-previous (tree node)
  (:documentation "Return the node with the next lower (aka previous) key in the tree"))

(defgeneric rb-keys (tree)
  (:documentation "Return all keys of the tree as an ordered list; not recommended for large trees"))

;; ---------------------------------------------------------------------------------------------------------------------
;; implementation
;; ---------------------------------------------------------------------------------------------------------------------

(defmethod rb-make-node ((tree red-black-tree) &key ((:key key) nil) ((:data data) nil))
  (make-instance (rb-node-class tree) :key key :data data))

(defmethod leafp ((tree red-black-tree) (node red-black-node))
  (eql node (leaf tree)))

(defmethod initialize-instance :after ((tree red-black-tree)  &key)
  (let ((leaf (rb-make-node tree)))
    (setf (color leaf) :black)
    (setf (parent leaf) leaf)
    (setf (left leaf) leaf)
    (setf (right leaf) leaf)
    (setf (leaf tree) leaf)
    (setf (root tree) leaf)))

(defmethod rb-insert ((tree red-black-tree) (node red-black-node))
  (let ((z node)
	(y (leaf tree))
	(x (root tree)))
    (loop until (leafp tree x)
       do (progn
	    (setf y x)
	    (cond ((rb< (key z) (key x))
		   (setf x (left x)))
		  (t
		   (setf x (right x))))))
    (setf (parent z) y)
    (cond ((leafp tree y)
	   (setf (root tree) z))
	  ((rb< (key z) (key y))
	   (setf (left y) z))
	  (t
	   (setf (right y) z)))
    (setf (left z) (leaf tree)
	  (right z) (leaf tree)
	  (color z) :red)
    (rb-insert-fixup tree z))
  node)

(defmethod rb-insert-fixup ((tree red-black-tree) (node red-black-node))
  (let ((z node))
    (loop while (eq (color (parent z)) :red)
       do (if (eq (parent z) (left (parent (parent z))))
	      ;; for when on left side
	      (let ((y (right (parent (parent z)))))
		(cond ((eq (color y) :red)
		       (setf (color (parent z)) :black)
		       (setf (color y) :black)
		       (setf (color (parent (parent z))) :red)
		       (setf z (parent (parent z))))
		      ((eq z (right (parent z)))
		       (setf z (parent z))
		       (rb-left-rotate tree z))
		      (t ;; is left child
		       (setf (color (parent z)) :black)
		       (setf (color (parent (parent z))) :red)
		       (rb-right-rotate tree (parent (parent z))))))
	      ;; for when on right side
	      (let ((y (left (parent (parent z)))))		
		(cond ((eq (color y) :red)
		       (setf (color (parent z)) :black)
		       (setf (color y) :black)
		       (setf (color (parent (parent z))) :red)
		       (setf z (parent (parent z))))
		      ((eq z (left (parent z)))
		       (setf z (parent z))
		       (rb-right-rotate tree z))
		      (t ;; is right child
		       (setf (color (parent z)) :black)
		       (setf (color (parent (parent z))) :red)
		       (rb-left-rotate tree (parent (parent z)))))))))
  (setf (color (root tree)) :black))

(defmethod rb-left-rotate ((tree red-black-tree) (node red-black-node))
  (let* ((x node)
	 (y (right x)))
    (setf (right x) (left y))
    (when (not (leafp tree (left y)))
	(setf (parent (left y)) x))
    (setf (parent y) (parent x))
    (cond ((leafp tree (parent x))
	   (setf (root tree) y))
	  ((eq x (left (parent x)))
	   (setf (left (parent x)) y))
	  (t 
	   (setf (right (parent x)) y)))
    (setf (left y) x)
    (setf (parent x) y)
    node))

(defmethod rb-right-rotate ((tree red-black-tree) (node red-black-node))
  (let* ((x node)
	 (y (left x)))
    (setf (left x) (right y))
    (when (not (leafp tree (right y)))
	(setf (parent (right y)) x))
    (setf (parent y) (parent x))
    (cond ((leafp tree (parent x))
	   (setf (root tree) y))
	  ((eq x (left (parent x)))
	   (setf (left (parent x)) y))
	  (t 
	   (setf (right (parent x)) y)))
    (setf (right y) x)
    (setf (parent x) y)
    node))

(defmethod rb-delete ((tree red-black-tree) (node red-black-node))
  (let* ((z node) 
	 (y z)
	 (y-original-color (color y))
	 x)
    (cond ((leafp tree (left z))
	   (setf x (right z))
	   (rb-transplant tree z (right z)))
	  ((leafp tree (right z))
	   (setf x (left z))
	   (rb-transplant tree z (left z)))
	  (t
	   (setf y (rb-tree-minimum tree (right z)))
	   (setf y-original-color (color y))
	   (setf x (right y))
	   (if (eq (parent y) z)
	       (setf (parent x) y)
	       (progn
		 (rb-transplant tree y (right y))
		 (setf (right y) (right z))
		 (setf (parent (right y)) y)))
	   (rb-transplant tree z y)
	   (setf (left y) (left z))
	   (setf (parent (left y)) y)
	   (setf (color y) (color z))))
    (when (eq y-original-color :black)
      (rb-delete-fixup tree x))))

(defmethod rb-transplant ((tree red-black-tree) (u red-black-node) (v red-black-node))
  (cond ((leafp tree (parent u))
	 (setf (root tree) v))
	((eq u (left (parent u)))
	 (setf (left (parent u)) v))
	(t 
	 (setf (right (parent u)) v)))
  ;; TODO something broken in the persistent case here, as 
  ;; we haven't detected that v is actally the leaf node,
  ;; so we try to modify it anyway
  ;; Adding test for safety--may be appropriate in the general case,
  ;; since entirely possible that v has arrived here as a leaf (no test in rb-delete, for example)
  (unless (leafp tree v)
    (setf (parent v) (parent u)))
  ;; (setf (parent v) (parent u))
  v)

(defmethod rb-delete-fixup ((tree red-black-tree) (node red-black-node))
  (let ((x node))
    (loop while (and (not (eq x (root tree)))
		     (eq (color x) :black)) 
	 do (if (eq x (left (parent x)))
	     ;; if left child
	     (let ((w (right (parent x))))
	       (when (eq (color w) :red)
		 (setf (color w) :black)
		 (setf (color (parent x)) :red)
		 (rb-left-rotate tree (parent x))
		 (setf w (right (parent x))))
	       (cond ((and (eq (color (left w)) :black)
			   (eq (color (right w)) :black))
		      (setf (color w) :red)
		      (setf x (parent x)))
		     ((eq (color (right w)) :black)
		      (setf (color (left w)) :black)
		      (setf (color w) :red)
		      (rb-right-rotate tree w)
		      (setf w (right (parent x))))
		     (t 
		      (setf (color w) (color (parent x)))
		      (setf (color (parent x)) :black)
		      (setf (color (right w)) :black)
		      (rb-left-rotate tree (parent x))
		      (setf x (root tree)))))
	     ;; if right child
	     (let ((w (left (parent x))))
	       (when (eq (color w) :red)
		 (setf (color w) :black)
		 (setf (color (parent x)) :red)
		 (rb-right-rotate tree (parent x))
		 (setf w (left (parent x))))
	       (cond ((and (eq (color (right w)) :black)
			   (eq (color (left w)) :black))
		      (setf (color w) :red)
		      (setf x (parent x)))
		     ((eq (color (left w)) :black)
		      (setf (color (right w)) :black)
		      (setf (color w) :red)
		      (rb-left-rotate tree w)
		      (setf w (left (parent x))))
		     (t 
		      (setf (color w) (color (parent x)))
		      (setf (color (parent x)) :black)
		      (setf (color (left w)) :black)
		      (rb-right-rotate tree (parent x))
		      (setf x (root tree)))))))
    (setf (color x) :black)))

(defmethod rb-tree-minimum ((tree red-black-tree) (node red-black-node))
  (unless (leafp tree node)
    (loop with x = node
       while (not (leafp tree (left x)))
       do (setf x (left x))
       finally (return x))))

(defmethod rb-tree-maximum ((tree red-black-tree) (node red-black-node))
  (unless (leafp tree node)
    (loop with x = node
       while (not (leafp tree (right x)))
       do (setf x (right x))
       finally (return x))))

(defmethod rb-find ((tree red-black-tree) key)
  (loop with node = (root tree)
     until (leafp tree node)
     for comparison = (rb-key-compare key (key node))
     until (eq :equal comparison)
     do (if (eq :less comparison)
	    (setf node (left node))
	    (setf node (right node)))
     finally (unless (leafp tree node)
	       (return node))))

(defmethod rb< (left right)
  (when (eq :less (rb-key-compare left right)) t))

(defmethod rb= (left right)
  (when (eq :equal (rb-key-compare left right)) t))

(defmethod rb-key-compare ((left number) (right number))
    (cond ((< left right) :less)
	  ((> left right) :greater)
	  (t :equal)))

(defmethod rb-key-compare ((left string) (right string))
    (cond ((string< left right) :less)
	  ((string> left right) :greater)
	  (t :equal)))

(defmethod rb-put ((tree red-black-tree) (key t) (data t))
  (when data ;; can't store nil
    (let ((node (rb-make-node tree :key key :data data)))
      (rb-insert tree node))))

(defmethod rb-get ((tree red-black-tree) key &optional (default nil))
  (let ((node (rb-find tree key)))
    (if node
	(values (data node) t)
	(values default nil))))

(defmethod rb-remove ((tree red-black-tree) key)
  (let ((node (rb-find tree key)))
    (when node
      (rb-delete tree node))))

(defmethod rb-size ((tree red-black-tree))
  (loop with count = 0
     with nodes = (list (root tree))
     while nodes
     for node = (pop nodes)
     unless (leafp tree node)
     do (progn
	  (incf count)
	  (push (left node) nodes)
	  (push (right node) nodes))
     finally (return count)))

(defmethod rb-first ((tree red-black-tree))
  (rb-tree-minimum tree (root tree)))

(defmethod rb-last ((tree red-black-tree))
  (rb-tree-maximum tree (root tree)))

(defmethod rb-next ((tree red-black-tree) (node red-black-node))
  (cond ((leafp tree node)
	 nil) 
	((not (leafp tree (right node)))
	 (rb-tree-minimum tree (right node)))
	(t (loop for start = node then (parent start)
	      if (leafp tree (parent start)) return nil
	      until (eq start (left (parent start)))
	      finally (return (parent start))))))

(defmethod rb-previous ((tree red-black-tree) (node red-black-node))
  (cond ((leafp tree node)
	 nil) 
	((not (leafp tree (left node)))
	 (rb-tree-maximum tree (left node)))
	(t (loop for start = node then (parent start)
	      if (leafp tree (parent start)) return nil
	      until (eq start (right (parent start)))
	      finally (return (parent start))))))

(defmacro with-rb-keys-and-data ((key-var data-var &optional (starting-point :first)) tree &rest body)
  (multiple-value-bind (starting-function incrementing-function)
      (cond ((eq starting-point :first)
	     (values 'rb-first 'rb-next))
	    ((eq starting-point :last)
	     (values 'rb-last 'rb-previous))
	    (t (error "Starting point should be either :first or :last")))
    `(loop with starting-node = (,starting-function ,tree)
	for node = starting-node then (,incrementing-function ,tree node)
	while node
	do (with-slots ((,key-var key) (,data-var data)) node
	     (declare (ignorable ,key-var ,data-var))
	     ,@body))))

(defmethod rb-keys ((tree red-black-tree))
  (let ((keys ()))
    (with-rb-keys-and-data (key data :first) tree
			   (setf keys (append keys (list key))))
    keys))

;; ---------------------------------------------------------------------------------------------------------------------
;; printing

(defmethod print-object ((obj red-black-tree) stream)
  (let ((*print-circle* t))
    (print-unreadable-object (obj stream :type t :identity t)
      (with-slots (root) obj
	(format stream "~_Root=~<~s~>" root)))))

(defmethod print-object ((obj red-black-node) stream)
  (let ((*print-circle* t))
    (with-slots (parent left right color key data) obj
      (print-unreadable-object (obj stream :type t :identity t)
	(with-slots (parent left right color key data) obj      
	  (if (eq obj parent)
	      (format stream "T.nil")
	      (format stream "Color=~s Key=~s Data=~s ~_Left=~<~s~> ~_Right=~<~s~>" color key data left right)))))))

