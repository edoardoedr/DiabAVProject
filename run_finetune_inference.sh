#!/bin/bash

BATCH_SIZE=16
EPOCHS=50
BLR=5e-3
NB_CLASSES=5
EVALUATION_ONLY= true
DATA_PATH="Datasets/IDRiD_data/"
TASK="runs/run_eval_idrid/"
RESUME=""
INPUT_SIZE=224
OUTPUT_DIR="runs/run_eval_idrid/"
LOG_DIR="runs/run_eval_idrid/"

EVAL_FLAG=""
if [ "$EVALUATION_ONLY" = true ]; then
  EVAL_FLAG="--eval"
fi

python -m torch.distributed.launch --nproc_per_node=1 --master_port=48798 DiabAVProject/RETFound/main_finetune.py \
    $EVAL_FLAG \
    --batch_size $BATCH_SIZE \
    --world_size 1 \
    --model vit_large_patch16 \
    --epochs $EPOCHS \
    --blr 5e-3 --layer_decay 0.65 \
    --weight_decay 0.05 --drop_path 0.2 \
    --nb_classes $NB_CLASSES \
    --data_path $DATA_PATH \
    --task $TASK \
    --resume $RESUME \
    --input_size $INPUT_SIZE \
    --output_dir $OUTPUT_DIR \
    --log_dir $LOG_DIR