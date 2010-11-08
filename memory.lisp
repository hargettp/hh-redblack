(in-package :rb-tree)

;; =====================================================================================================================
;;
;; memory-based red-black tree
;;
;; ---------------------------------------------------------------------------------------------------------------------

;; ---------------------------------------------------------------------------------------------------------------------
;; types
;; ---------------------------------------------------------------------------------------------------------------------
(defclass memory-red-black-node (red-black-node)
  ((parent :initform nil :accessor parent)
   (left :initform nil :accessor left)
   (right :initform nil :accessor right)
   (color :initform nil :accessor color)
   (key :initform nil :initarg :key :accessor key)
   (data :initform nil :initarg :data :accessor data)))

(defclass memory-red-black-tree (red-black-tree)
  ((root :accessor root)
   (leaf :accessor leaf)))

;; ---------------------------------------------------------------------------------------------------------------------
;; implementation
;; ---------------------------------------------------------------------------------------------------------------------

(defun make-red-black-tree ()
  (make-instance 'memory-red-black-tree))

(defmethod rb-node-class ((tree memory-red-black-tree))
  'memory-red-black-node)

