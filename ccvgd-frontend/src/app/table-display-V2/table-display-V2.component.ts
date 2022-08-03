import {SelectionModel} from '@angular/cdk/collections';
import {FlatTreeControl} from '@angular/cdk/tree';
import {Component, Input, OnInit} from '@angular/core';
import {MatTreeFlatDataSource, MatTreeFlattener} from '@angular/material/tree';
import {TableData} from '../services/village-name.service';
import {debounceTime} from 'rxjs/operators';


/**
 * @title Tree with checkboxes
 */

@Component({
  selector: 'app-table-display-V2',
  templateUrl: './table-display-V2.component.html',
  styleUrls: ['./table-display-V2.css'],
})
export class TableDisplayV2Component implements OnInit {
  dataChange: TodoItemNode[];

  flatNodeMap = new Map<TodoItemFlatNode, TodoItemNode>();

  /** Map from nested node to flattened node. This helps us to keep the same object for selection */
  nestedNodeMap = new Map<TodoItemNode, TodoItemFlatNode>();

  treeControl: FlatTreeControl<TodoItemFlatNode>;
  treeFlattener: MatTreeFlattener<TodoItemNode, TodoItemFlatNode>;

  dataSource: MatTreeFlatDataSource<TodoItemNode, TodoItemFlatNode>;

  /** The selection for checklist */
  checklistSelection = new SelectionModel<TodoItemFlatNode>(true /* multiple */);
  // 储存选择项进行filter
  selectionSaveList: TodoItemFlatNode[][];

  filteredList: any;
  fulllist: any;
  @Input('tabledata') table: TableData; //note: @Input property 必须要使用在OnInit里面才可以,constructor里面会报错

  constructor() {
    // 一个含有4个function的object
    this.treeFlattener = new MatTreeFlattener(this.transformer, this.getLevel,
      this.isExpandable, this.getChildren);
    //console.log("treeFlattener",this.treeFlattener);

    this.treeControl = new FlatTreeControl<TodoItemFlatNode>(this.getLevel, this.isExpandable);
    //console.log("treeControl",this.treeControl);
    this.dataSource = new MatTreeFlatDataSource(this.treeControl, this.treeFlattener);
  }

  getLevel = (node: TodoItemFlatNode) => node.level;

  isExpandable = (node: TodoItemFlatNode) => node.expandable;

  getChildren = (node: TodoItemNode): TodoItemNode[] => node.children;

  hasChild = (_: number, _nodeData: TodoItemFlatNode) => _nodeData.expandable;

  //hasNoContent = (_: number, _nodeData: TodoItemFlatNode) => _nodeData.description === '';

  /**
   * Transformer to convert nested node to flat node. Record the nodes in maps for later use.
   */
  transformer = (node: TodoItemNode, level: number) => {
    const existingNode = this.nestedNodeMap.get(node);
    // console.log("existingNode",existingNode);
    const flatNode = existingNode && existingNode.description === node.description
      ? existingNode
      : new TodoItemFlatNode();
    flatNode.description = node.description;
    flatNode.level = level;
    flatNode.expandable = !!node.children?.length;
    this.flatNodeMap.set(flatNode, node);
    this.nestedNodeMap.set(node, flatNode);
    return flatNode;
  }

  /** Whether all the descendants of the node are selected. */
  // 选中父checkbox的时候会全部选中儿子们
  descendantsAllSelected(node: TodoItemFlatNode): boolean {
    const descendants = this.treeControl.getDescendants(node);
    // console.log("descendants",descendants);
    const descAllSelected = descendants.length > 0 && descendants.every(child => {
      return this.checklistSelection.isSelected(child);
    });
    return descAllSelected;
  }

  /** Whether part of the descendants are selected */
  descendantsPartiallySelected(node: TodoItemFlatNode): boolean {
    const descendants = this.treeControl.getDescendants(node);
    // console.log("descendants",descendants);
    const result = descendants.some(child => this.checklistSelection.isSelected(child));
    // console.log("result",result);
    return result && !this.descendantsAllSelected(node);
  }

  /** Toggle the to-do item selection. Select/deselect all the descendants node */
  todoItemSelectionToggle(node: TodoItemFlatNode): void {
    this.checklistSelection.toggle(node);
    const descendants = this.treeControl.getDescendants(node);
    this.checklistSelection.isSelected(node)
      ? this.checklistSelection.select(...descendants)
      : this.checklistSelection.deselect(...descendants);

    // Force update for the parent
    descendants.forEach(child => this.checklistSelection.isSelected(child));
    this.checkAllParentsSelection(node);
  }

  /** Toggle a leaf to-do item selection. Check all the parents to see if they changed */
  todoLeafItemSelectionToggle(node: TodoItemFlatNode): void {
    this.checklistSelection.toggle(node);
    this.checkAllParentsSelection(node);
  }

  /* Checks all the parents when a leaf node is selected/unselected */
  checkAllParentsSelection(node: TodoItemFlatNode): void {
    let parent: TodoItemFlatNode | null = this.getParentNode(node);
    while (parent) {
      this.checkRootNodeSelection(parent);
      parent = this.getParentNode(parent);
    }
  }

  /** Check root node checked state and change it accordingly */
  checkRootNodeSelection(node: TodoItemFlatNode): void {
    const nodeSelected = this.checklistSelection.isSelected(node);
    const descendants = this.treeControl.getDescendants(node);
    const descAllSelected = descendants.length > 0 && descendants.every(child => {
      return this.checklistSelection.isSelected(child);
    });
    if (nodeSelected && !descAllSelected) {
      this.checklistSelection.deselect(node);
    } else if (!nodeSelected && descAllSelected) {
      this.checklistSelection.select(node);
    }
  }

  /* Get the parent node of a node */
  getParentNode(node: TodoItemFlatNode): TodoItemFlatNode | null {
    const currentLevel = this.getLevel(node);

    if (currentLevel < 1) {
      return null;
    }

    const startIndex = this.treeControl.dataNodes.indexOf(node) - 1;

    for (let i = startIndex; i >= 0; i--) {
      const currentNode = this.treeControl.dataNodes[i];

      if (this.getLevel(currentNode) < currentLevel) {
        return currentNode;
      }
    }
    return null;
  }

  /**
   * Build the file structure tree. The `value` is the Json object, or a sub-tree of a Json object.
   * The return value is the list of `TodoItemNode`.
   */
  buildFileTree(obj: {[key: string]: any}, level: number): TodoItemNode[] {
    return Object.keys(obj).reduce<TodoItemNode[]>((accumulator, key) => {
      const value = obj[key];
      console.log("value",value);
      const node = new TodoItemNode();
      node.description = key;

      if (value != null) {
        if (typeof value === 'object') {
          node.children = this.buildFileTree(value, level + 1);
        } else {
          node.description = key;
        }
      }

      return accumulator.concat(node);
    }, []);
  }

  ngOnInit(): void {
    //console.log("this.table from table-display-V2", this.table);
    this.dataChange = this.buildFileTree(this.table.treeFilter, 0);
    this.dataSource.data = this.dataChange;

    this.filteredList = this.table.data;
    this.fulllist = this.table.data;
    console.log("tble display V2 this.table",this.table);

    this.checklistSelection.changed.pipe(
      debounceTime(10)
    ).subscribe((value) => {
      //console.log("value",value);
      //console.log("selected",this.checklistSelection.selected);

      let selected = this.checklistSelection.selected;

      // 这里可以用recursion优化，目前只有三层的搜索
      this.filteredList = this.fulllist.filter(row => {

        for(let node of selected){
          // category 1
          if(node.level == 0 && row.category1.includes(node.description)){
              return true;
          }
          // category 2
          if(node.level == 1){
            if(row.category1.includes(this.getParentNode(node).description)
               && row.category2.includes(node.description)){
              return true;
            }
          }
          // category 3
          if(node.level == 2){
            let cat2 = this.getParentNode(node);
            let cat1 = this.getParentNode(cat2);
            if(row.category1.includes(cat1.description)
               && row.category2.includes(cat2.description)
               && row.category3.includes(node.description)){
              return true;
            }

          }
        }

      }); // end of table-display-V2

      // no table-display-V2 applied
      if(this.checklistSelection.selected.length == 0){
        this.filteredList = this.fulllist;
      }

    });

  }
}

/**
 * Node for to-do item
 */
export class TodoItemNode {
  children: TodoItemNode[];
  description: string; // Chinese + English
  type: string;
}

/** Flat to-do item node with expandable and level information */
export class TodoItemFlatNode {
  description: string;
  level: number;
  expandable: boolean;
  type: string;
}


