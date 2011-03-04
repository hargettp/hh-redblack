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
;; persistent red-black tree
;;
;; ---------------------------------------------------------------------------------------------------------------------


;; ---------------------------------------------------------------------------------------------------------------------
;; types
;; ---------------------------------------------------------------------------------------------------------------------

(deftype object-unloaded () `(eql :unloaded))

(deftype object-loaded () `(or (eql :loaded)
			     (eql :changed)
			     (eql :new)))

(deftype node-state () `(or object-unloaded
			   object-loaded))

(defclass persistent-red-black-object ()
  ((state :type node-state :initform :new :initarg :state :accessor state) 
   (location :initform 0 :initarg :location :accessor location))) 

(defclass persistent-red-black-node (red-black-node persistent-red-black-object)
  ((left :initform nil :accessor left)
   (right :initform nil :accessor right)
   (color :initform nil :accessor color)
   (key :initform nil :initarg :key :accessor key)
   (data :initform nil :initarg :data :accessor data)))

(defclass persistent-red-black-data (persistent-red-black-object)
  ((contents :initform nil :initarg :contents :accessor contents)))

(defclass persistent-red-black-tree (red-black-tree)
  ((root :initform nil :accessor root)
   (leaf :initform nil :accessor leaf)))

(defclass red-black-tree-transaction ()
  ((tree :initarg :tree :accessor tree) 
   (new-root :initform nil :accessor new-root)
   (parents :initform (make-hash-table ) :accessor parents :documentation "For mapping objects to their parent objects")
   (changes :initform (make-array 0 :adjustable t :fill-pointer t) :accessor changes)))

;; ---------------------------------------------------------------------------------------------------------------------
;; variables
;; ---------------------------------------------------------------------------------------------------------------------

(defvar *rb-transaction* nil 
  "The currently active transaction on a red-black tree")

;; ---------------------------------------------------------------------------------------------------------------------
;; generics
;; ---------------------------------------------------------------------------------------------------------------------

(defgeneric rb-data-class (tree)
  (:documentation "Return the class to be used for storing data in the tree"))

(defgeneric rb-make-data (tree &key location contents))

(defgeneric add-new-object (transaction object))

(defgeneric add-changed-object (transaction object))

(defgeneric add-child-object (transaction parent child))

(defgeneric changedp (transaction object))

(defgeneric prb-open-storage (tree)
  (:documentation "Prepare tree for use; after this call load & save operations should succeed, the root should be loaded, 
    and the leaf sentinel identified"))

(defgeneric prb-close-storage (storage)
  (:documentation "Release storage from use; further load & save operations cannot succeed without a subsequent open call"))

(defgeneric prb-leaf-location-p (tree location)
  (:documentation "Return true if the location points to the nil or leaf sentinel"))

(defgeneric prb-location (tree)
  (:documentation "Return the current location within storage where new objects would be written"))

(defgeneric prb-load-node (tree node)
  (:documentation "Load the node's state from storage, if not already loaded"))

(defgeneric prb-load-data (tree data)
  (:documentation "Load data from storage, if not already loaded"))

(defgeneric prb-fetch-node (tree location)
  (:documentation "Return the state of a node as multiple values:
     -- location of left child, or location for nil sentinel
     -- location of right child, or location for nil sentinel
     -- color value
     -- key value
     -- location of data"))

(defgeneric prb-fetch-data (tree location)
  (:documentation "Return data from the indicated location"))

(defgeneric refresh-node (tree node)
  (:documentation "Refresh a node's state from storage, if necessary"))

(defgeneric prb-save-node (tree node)
  (:documentation "Save the indicated node in storage, using prb-stash-node. Tree implementations
   are free to implement either prb-save-node (high-level) or prb-stash-node (low-level), depending
   on their needs.  Typical implementations may want to implement prb-stash-node to write their own
   storage, but implementing prb-save-node would permit extending what node state is saved."))

(defgeneric prb-stash-node (tree left-location right-location color-value key-value data-location)
  (:documentation "Save node state into storage"))

(defgeneric prb-stash-data (tree contents)
  (:documentation "Save the indicated contents of a data object in storage"))

(defgeneric prb-save-data (tree data)
  (:documentation "Save the indicated data in storage"))

(defgeneric prb-save-object (tree object)
  (:method ((tree persistent-red-black-tree) (node persistent-red-black-node))
    (prb-save-node tree node))
  (:method ((tree persistent-red-black-tree) (object t))
    (prb-save-data tree object)))

(defgeneric prb-save-root (tree root)
  (:documentation "Save the indicated root to storage; after this call, any subsequent
    transaction should see this object as the root of the tree"))

(defgeneric prb-commit (transaction-or-tree)
  (:documentation "Orchestrate the persisting of all changes to a tree, including all changed nodes"))

(defgeneric prb-abort (transaction-or-tree)
  (:documentation "Abandon any changes in the tree; note that any nodes held should be reacquired after an abort"))

;; ---------------------------------------------------------------------------------------------------------------------
;; implementation
;; ---------------------------------------------------------------------------------------------------------------------

(define-condition requires-red-black-transaction ()
  ()
  (:report (lambda (condition stream)
	     (declare (ignorable condition))
	    (format stream "Accessing a persistent red-black tree requires a transaction; wrap code in a with-rb-transaction form"))))

(define-condition transaction-aborted ()
  ()
  (:report (lambda (condition stream)
	     (declare (ignorable condition))
	     (format stream "Transaction aborted"))))

(defun require-rb-transaction ()
  (unless *rb-transaction*
    (error 'requires-red-black-transaction)))

(defun clear-changes ()
  (loop with changes = (changes *rb-transaction*)
     until (= 0 (length changes))
     do (vector-pop changes)))

(defgeneric loaded-p (object)
  (:method ((object persistent-red-black-object))
    (typep (state object) 'object-loaded)))

(defgeneric ancestor-p (node possible-ancestor)
  (:method ((node persistent-red-black-node) (possible-ancestor persistent-red-black-node))
    (let ((tree (tree *rb-transaction*))) 
      (cond ((leafp tree node)
	     nil) 
	    ((leafp tree possible-ancestor)
	     t)
	    ((eq node possible-ancestor)
	     nil)
	    ((eq (parent node) possible-ancestor)
	     t)
	    (t
	     (ancestor-p (parent node) possible-ancestor))))))

(macrolet ((declare-slot-translation (slot)
	     `(progn
		(defmethod ,slot :around ((node persistent-red-black-node))
		  (require-rb-transaction)
		  (prb-load-node (tree *rb-transaction*) node)
		  (call-next-method))

		(defmethod (setf ,slot) :around (value (node persistent-red-black-node))
		  (require-rb-transaction)
		  (prb-load-node (tree *rb-transaction*) node)
		  (add-changed-object *rb-transaction* node)
		  (add-child-object *rb-transaction* node value)
		  (call-next-method value node)))))

  (declare-slot-translation left)
  (declare-slot-translation right)
  (declare-slot-translation key)
  (declare-slot-translation color))

(defmethod data :around ((node persistent-red-black-node))
  (require-rb-transaction)
  (prb-load-node (tree *rb-transaction*) node)
  (contents (call-next-method)))

(defmethod (setf data) :around (value (node persistent-red-black-node))
  (require-rb-transaction)
  (prb-load-node (tree *rb-transaction*) node)
  (add-changed-object *rb-transaction* node)
  (let ((data (rb-make-data (tree *rb-transaction*) :contents value)))
    (call-next-method data node)))

(defmethod contents :around ((data persistent-red-black-data))
  (require-rb-transaction)
  (prb-load-data (tree *rb-transaction*) data)
  (call-next-method))

(defmethod parent ((node persistent-red-black-node))
  (require-rb-transaction)
  (let ((tree (tree *rb-transaction*)))
    (if (leafp tree node)
	(leaf tree)
	(or (gethash node (parents *rb-transaction*))
	    (leaf tree)))))

(defmethod (setf parent) (value (node persistent-red-black-node))
  (require-rb-transaction)
  (add-changed-object *rb-transaction* node)
  (add-child-object *rb-transaction* value node))

(defmethod (setf color) :around (color (node persistent-red-black-node))
  (require-rb-transaction)
  (add-changed-object *rb-transaction* node)
  (call-next-method))

(defmethod root :around ((tree persistent-red-black-tree))
  (require-rb-transaction)
  (or (and *rb-transaction* (new-root *rb-transaction*))
      (call-next-method)))

(defmethod (setf root) (node (tree persistent-red-black-tree))
  (require-rb-transaction)
  (add-changed-object *rb-transaction* node)
  ;; TODO on commit should change the in-memory tree, too?
  (setf (new-root *rb-transaction*) node))

(defmethod (setf leaf) :around (node (tree persistent-red-black-tree))
  (require-rb-transaction)
  (add-new-object *rb-transaction* node)
  (call-next-method))

(defmethod rb-make-node :around ((tree persistent-red-black-tree) &key ((:key key) nil) ((:data data) nil))
  (declare (ignorable key data))
  (require-rb-transaction)
  (let ((node (call-next-method tree :key key :data (when data (rb-make-data tree :contents data)))))
    (add-new-object *rb-transaction* node)
    node))

(defmethod rb-make-data ((tree persistent-red-black-tree) &key location contents)
  (require-rb-transaction)
  (let ((data (make-instance (rb-data-class tree) :location location :contents contents)))
    (add-new-object *rb-transaction* data)
    data))

(defmethod leafp ((tree persistent-red-black-tree) (node persistent-red-black-node))
  (eql node (leaf tree)))

(defmethod add-new-object ((*rb-transaction* red-black-tree-transaction) object)
  (unless (changedp *rb-transaction* object)
    (vector-push-extend object (changes *rb-transaction*)))
  object)

(defmethod add-changed-object ((*rb-transaction* red-black-tree-transaction) object)
  (when object
    (unless (leafp (tree *rb-transaction*) object)
      (unless (changedp *rb-transaction* object)
	(vector-push-extend object (changes *rb-transaction*))
	;; now make sure all ancestors up to root are changed
	(add-changed-object *rb-transaction* (parent object)))))
  object)

(defmethod add-child-object ((*rb-transaction* red-black-tree-transaction) (parent persistent-red-black-node) (child persistent-red-black-object))
  (setf (gethash child (parents *rb-transaction*)) parent)
  parent)

(defmethod add-child-object ((*rb-transaction* red-black-tree-transaction) (parent persistent-red-black-node) (child t))
  ;; this may be a bit subtle--basically, the reason this is here is so that the code for key and color
  ;; slot translations above (see declare-slot-translation macrolet) can be identical to left and right,
  ;; but without any attempt to track the parent of what amounts to a value not a persistent object
  parent)

(defmethod changedp ((*rb-transaction* red-black-tree-transaction) object)
  (find object (changes *rb-transaction*) :test #'eql))

(defmacro with-rb-transaction ((tree) &rest body)
  `(let* ((existing-transaction *rb-transaction*) 
	  (*rb-transaction* (or existing-transaction (make-instance 'red-black-tree-transaction))))
     (setf (tree *rb-transaction*) ,tree)
     (prb-open-storage (tree *rb-transaction*))
     (unwind-protect
	  (handler-bind ((error #'(lambda (e)
				    (declare (ignorable e))
				    (prb-abort *rb-transaction*))))
	    (let ((v (multiple-value-list (progn 
					    ,@body))))
	      (unless existing-transaction (prb-commit *rb-transaction*))
	      (values-list v)))
       (prb-close-storage (tree *rb-transaction*)))))

(defmethod initialize-instance :before ((tree persistent-red-black-tree)  &key)
  (setf (tree *rb-transaction*) tree))

(defmethod rb-node-class ((tree persistent-red-black-tree))
  'persistent-red-black-node)

(defmethod rb-data-class ((tree persistent-red-black-tree))
  'persistent-red-black-data)

(defmethod prb-load-node ((tree persistent-red-black-tree) (node persistent-red-black-node))
  (unless (loaded-p node)
    (flet ((open-node (location)
	     (if (prb-leaf-location-p tree location)
		 (leaf tree)
		 (let ((child (make-instance (rb-node-class tree))))
		   (setf (location child) location)
		   (setf (state child) :unloaded)
		   (add-child-object *rb-transaction* node child)
		   child))))
      (multiple-value-bind (left-location right-location color-value key-value data-location) 
	  (prb-fetch-node tree (location node))
	(with-slots (left right color key data) node
	  (setf left (open-node left-location)
		right (open-node right-location)
		color color-value
		key key-value
		data (when data-location (let ((data-object (make-instance (rb-data-class tree) :location data-location)))
					   (setf (state data-object) :unloaded)
					   data-object)))))
      (setf (state node) :loaded)))
  node)

(defmethod prb-save-node ((tree persistent-red-black-tree) (node persistent-red-black-node))
  ;; since it's possible that the node has not been loaded (e.g., if it was an ancestor of
  ;; a changed node), make sure it is loaded first
  ;; NOTE: this may limit some implementations, as this code assumes direct slot access is
  ;; valid after loading--thus short-circuiting slot accessors
  (prb-load-node tree node)
  (with-slots (left right color key data) node
    (prb-stash-node tree (location left) (location right) color key (when data (location data)))))

(defmethod prb-load-data ((tree persistent-red-black-tree) (data persistent-red-black-data))
  (unless (loaded-p data)
    (setf (contents data) (prb-fetch-data tree (location data)))))

(defmethod prb-save-data ((tree persistent-red-black-tree) (data persistent-red-black-data))
  (prb-stash-data tree (contents data)))

(defmethod prb-abort ((*rb-transaction* red-black-tree-transaction))
  (initialize-instance *rb-transaction*))

(defgeneric compare-save-order (left right)
  (:method ((left persistent-red-black-node) (right persistent-red-black-node))
    (cond ((ancestor-p left right)
	   t)
	  (t nil)))
  (:method ((left t) (right persistent-red-black-node))
    t)
  (:method ((left persistent-red-black-node) (right t))
    nil)
  (:method ((left t) (right t))
    nil))

(defun sort-into-save-order (changes)
  (let ((sorted-changes ())
	(unsorted-changes (loop for change across changes collect change))
	(next-unsorted-changes ()))
    (loop while unsorted-changes   
       do (loop for change in unsorted-changes
	     if (loop for other in unsorted-changes
		   if (compare-save-order other change) return nil
		   finally (return t))
	     do (push change sorted-changes)
	     else do (push change next-unsorted-changes))
       do (setf unsorted-changes next-unsorted-changes)
       do (setf next-unsorted-changes ()))
    (reverse sorted-changes)))

(defmethod prb-commit ((*rb-transaction* red-black-tree-transaction))
  (let* ((tree (tree *rb-transaction*))
	 ;; sort changed objects so that objects are written before references
	 ;; to them need to be written (thus, leaves first)
	 (sorted-changes (sort-into-save-order (changes *rb-transaction*))))
    (loop for object in sorted-changes
       do (let ((location (prb-location tree)))
	    (prb-save-object tree object)
	    ;; update its location after save--other objects referencing it
	    ;; will now save a reference to the new location
	    (setf (location object) location)))
    ;; only save the root if there are changes
    (when sorted-changes (prb-save-root tree (root tree)))))


;; ---------------------------------------------------------------------------------------------------------------------
;; printing

(defmethod print-object ((obj persistent-red-black-node) stream)
  (let ((*print-circle* t))
    (print-unreadable-object (obj stream :type t :identity t)
      (with-slots (location left right color key data state) obj
	(format stream "Location=~s Color=~s Key=~s Data=~s ~_Left=~<~s~> ~_Right=~<~s~> State=~s" location color key data left right state)))))

(defmethod print-object ((obj persistent-red-black-data) stream)
  (let ((*print-circle* t))
    (print-unreadable-object (obj stream :type t :identity t)
      (with-slots (location contents) obj
	(format stream "Location=~s Contents=~s" location contents)))))